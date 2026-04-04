'use strict';

const express = require('express');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const { trackBlock } = require('./track_block');

const PORT = Number(process.env.PORT || 7860);
const RUN_TIMEOUT_MS = Number(process.env.RUN_TIMEOUT_MS || 25000);
const FALLBACK_TIMEOUT_MS = Number(process.env.FALLBACK_TIMEOUT_MS || 90000);
const TRACK_CONCURRENCY = Math.max(1, Number(process.env.TRACK_CONCURRENCY || 6));
const ENABLE_PLAYWRIGHT_FALLBACK = process.env.ENABLE_PLAYWRIGHT_FALLBACK === '1';

const pendingByBlock = new Map();
const queue = [];
let activeWorkers = 0;

const app = express();
app.use(express.json({ limit: '100kb' }));
app.use(express.static(path.join(__dirname, 'public')));

function normalizeBlock(input) {
  return String(input || '').trim().toUpperCase();
}

function isLikelyBlock(block) {
  return /^[0-9]{1,3}-[0-9]{1,3}$/.test(block);
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      const err = new Error(`Live lookup timed out after ${ms}ms`);
      err.code = 504;
      setTimeout(() => reject(err), ms);
    }),
  ]);
}

function sha1(value) {
  return crypto.createHash('sha1').update(value).digest('hex');
}

function httpGetText(url, timeoutMs = RUN_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        if (res.statusCode >= 400) {
          const err = new Error(`HTTP ${res.statusCode} for ${url}`);
          err.code = res.statusCode;
          reject(err);
          return;
        }
        resolve(data);
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Request timeout for ${url}`));
    });

    req.on('error', reject);
  });
}

async function fetchTransSeeText(url, timeoutMs = RUN_TIMEOUT_MS) {
  const firstHtml = await httpGetText(url, timeoutMs);
  if (!/Proof of work - TransSee/i.test(firstHtml)) {
    return firstHtml;
  }

  const powMatch = firstHtml.match(/process\('([^']+)',\s*(\d+)\)/);
  if (!powMatch) {
    return firstHtml;
  }

  const [, seed, difficultyText] = powMatch;
  const difficulty = Number(difficultyText);
  const prefix = '0'.repeat(Number.isFinite(difficulty) ? difficulty : 0);
  let nonce = 0;
  let solved = null;

  while (nonce < 2000000) {
    const candidate = `${seed}${nonce}`;
    if (sha1(candidate).startsWith(prefix)) {
      solved = candidate;
      break;
    }
    nonce += 1;
  }

  if (!solved) {
    return firstHtml;
  }

  const retryUrl = new URL(url);
  retryUrl.searchParams.set(
    'ua',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36'
  );
  retryUrl.searchParams.set('pw', solved);
  return httpGetText(retryUrl.toString(), timeoutMs);
}

function getOttawaServiceDateIso() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());

  const map = {};
  for (const p of parts) map[p.type] = p.value;
  return `${map.year}-${map.month}-${map.day}T10:00:00.000Z`;
}

function getOttawaServiceDateString() {
  return getOttawaServiceDateIso().slice(0, 10);
}

function timeToSeconds(value) {
  const t = String(value || '').trim();
  const m = t.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  const ss = Number(m[3] || 0);
  return hh * 3600 + mm * 60 + ss;
}

function pickMostRecentBusId(trips) {
  const candidates = [];
  for (const trip of trips || []) {
    const busId = String(trip && trip.busId ? trip.busId : '').trim();
    if (!/^\d{3,5}$/.test(busId)) continue;

    const actualEnd = timeToSeconds(trip.actualEndTime);
    const actualStart = timeToSeconds(trip.actualStartTime);
    const scheduledStart = timeToSeconds(trip.scheduledStartTime);

    // Prefer trips with real actual telemetry. Fall back to schedule only when needed.
    const hasActual = actualEnd !== null || actualStart !== null;
    const rank = hasActual ? (actualEnd ?? actualStart) : (scheduledStart ?? -1);

    candidates.push({
      busId,
      hasActual: hasActual ? 1 : 0,
      rank,
      tie: actualStart ?? actualEnd ?? scheduledStart ?? -1,
    });
  }

  candidates.sort((a, b) =>
    b.hasActual - a.hasActual ||
    b.rank - a.rank ||
    b.tie - a.tie ||
    b.busId.localeCompare(a.busId, undefined, { numeric: true })
  );

  return candidates.length ? candidates[0].busId : null;
}

function decodeEntities(s) {
  return s
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/gi, '"');
}

function htmlToLines(html) {
  const noScript = html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ');
  const text = decodeEntities(noScript.replace(/<[^>]+>/g, '\n'));
  return text
    .split('\n')
    .map((line) => line.replace(/\s+/g, ' ').trim())
    .filter(Boolean);
}

function pickBestLocationLine(lines, busNumber) {
  const cleaned = lines
    .filter((line) => line.length >= 8 && line.length <= 260)
    .filter((line) => !/vehicle locations - .* - transsee/i.test(line));

  const precise = cleaned
    .filter((line) => {
      const lower = line.toLowerCase();
      if (lower.includes('near stops by gps')) return false;
      if (/(privacy|copyright|transsee by|search|menu|map|vehicle locations)/i.test(lower)) return false;
      const hasMarker = /\b(aprchg|approach|approaching|past|near|at|arriving)\b/i.test(lower);
      if (!hasMarker) return false;
      return true;
    })
    .sort((a, b) => b.length - a.length);

  if (precise.length > 0) {
    return precise[0].replace(/\s+Last seen.*$/i, '').trim();
  }

  const scored = cleaned
    .map((line) => {
      const lower = line.toLowerCase();
      let score = 0;
      if (line.includes(busNumber)) score += 55;
      if (lower.includes(' on ')) score += 28;
      if (lower.includes(' going ')) score += 24;
      if (lower.startsWith('vehicle ') || lower.includes(`vehicle ${busNumber}`)) score += 18;
      if (/[↑↓↗↘↖↙]/.test(line)) score += 5;
      if (lower.includes('near stops by gps')) score -= 100;
      if (/(privacy|copyright|transsee by|search|menu|map|vehicle locations)/i.test(lower)) score -= 20;
      score += Math.min(line.length, 140) / 14;
      return { line, score };
    })
    .filter((x) => x.score >= 20)
    .sort((a, b) => b.score - a.score || b.line.length - a.line.length);

  if (scored.length > 0) {
    return scored[0].line.replace(/\s+Last seen.*$/i, '').trim();
  }

  return null;
}

async function fetchBusesForBlock(block) {
  const dateIso = getOttawaServiceDateIso();
  const detailsUrl = `https://bus.ajay.app/api/blockDetails?blockId=${encodeURIComponent(block)}&date=${encodeURIComponent(dateIso)}`;

  let payload;
  try {
    payload = JSON.parse(await httpGetText(detailsUrl));
  } catch (err) {
    throw new Error(`Failed to read BetterTransit data: ${err.message}`);
  }

  const trips = payload && payload[block] ? payload[block] : null;
  if (!Array.isArray(trips)) {
    throw Object.assign(new Error(`Block not found: ${block}`), { code: 404 });
  }

  if (trips.length === 0) {
    return [];
  }

  const mostRecentBus = pickMostRecentBusId(trips);
  if (!mostRecentBus) {
    return [];
  }

  return [mostRecentBus];
}

async function fetchTripsForBlock(block) {
  const dateIso = getOttawaServiceDateIso();
  const detailsUrl = `https://bus.ajay.app/api/blockDetails?blockId=${encodeURIComponent(block)}&date=${encodeURIComponent(dateIso)}`;

  let payload;
  try {
    payload = JSON.parse(await httpGetText(detailsUrl));
  } catch (err) {
    throw new Error(`Failed to read BetterTransit data: ${err.message}`);
  }

  const trips = payload && payload[block] ? payload[block] : null;
  if (!Array.isArray(trips)) {
    throw Object.assign(new Error(`Block not found: ${block}`), { code: 404 });
  }

  return trips;
}

async function fetchAvailableBlocks() {
  const dateIso = getOttawaServiceDateIso();
  const blocksUrl = `https://bus.ajay.app/api/blocks?date=${encodeURIComponent(dateIso)}`;
  let payload;
  try {
    payload = JSON.parse(await httpGetText(blocksUrl));
  } catch (err) {
    throw new Error(`Failed to read block list: ${err.message}`);
  }
  if (!Array.isArray(payload)) {
    throw new Error('Invalid block list payload');
  }
  return payload
    .map((row) => String(row && row.blockId ? row.blockId : '').trim().toUpperCase())
    .filter(Boolean);
}

function blockNumericKey(block) {
  const [a, b] = String(block || '').split('-');
  if (!/^\d+$/.test(a || '') || !/^\d+$/.test(b || '')) return null;
  return `${Number(a)}-${Number(b)}`;
}

async function resolveCanonicalBlock(inputBlock) {
  const available = await fetchAvailableBlocks();
  const exact = available.find((b) => b === inputBlock);
  if (exact) return exact;

  const inputKey = blockNumericKey(inputBlock);
  if (!inputKey) return null;

  const keyToCanonical = new Map();
  for (const b of available) {
    const key = blockNumericKey(b);
    if (key && !keyToCanonical.has(key)) keyToCanonical.set(key, b);
  }
  return keyToCanonical.get(inputKey) || null;
}

async function fetchLocationForBus(busNumber) {
  const url = `https://transsee.ca/fleetfind?a=octranspo&q=${encodeURIComponent(busNumber)}&Go=Go`;
  const html = await fetchTransSeeText(url);
  const lines = htmlToLines(html);
  const locationText = pickBestLocationLine(lines, busNumber);

  if (!locationText) {
    throw Object.assign(new Error(`No location found for bus ${busNumber}`), { code: 404 });
  }

  return {
    busNumber: String(busNumber),
    locationText,
    url,
  };
}

function getTripCandidatePriority(trip) {
  const actualStart = timeToSeconds(trip.actualStartTime);
  const actualEnd = timeToSeconds(trip.actualEndTime);
  const scheduledStart = timeToSeconds(trip.scheduledStartTime);

  if (actualStart !== null && actualEnd === null) {
    return 300000 + actualStart;
  }
  if (actualStart !== null) {
    return 200000 + actualStart;
  }
  if (scheduledStart !== null) {
    return 100000 + scheduledStart;
  }
  return 0;
}

function getTripCandidatesForTransSee(trips) {
  return [...trips]
    .filter((trip) => trip && trip.tripId)
    .sort((a, b) => getTripCandidatePriority(b) - getTripCandidatePriority(a))
    .slice(0, 5);
}

function extractBusNumberFromTripSched(html) {
  const blockSectionMatch = html.match(/<div id=block><h4>Trips in this block<\/h4>([\s\S]*?)<\/table><\/div>/i);
  if (!blockSectionMatch) return null;

  const section = blockSectionMatch[1];
  const currentRowMatch = section.match(/<tr><td>[\s\S]*?<td style="font-weight:\s*bold;">[\s\S]*?<td>([\s\S]*?)<\/td><\/tr>/i);
  if (!currentRowMatch) return null;

  const tripPathCell = currentRowMatch[1];
  const busMatch = tripPathCell.match(/>(\d{3,5})<\/a>/);
  return busMatch ? busMatch[1] : null;
}

async function fetchBusFromTransSeeTrip(trip) {
  const date = getOttawaServiceDateString();
  const url = `https://www.transsee.ca/tripsched?a=octranspo&t=${encodeURIComponent(trip.tripId)}&date=${encodeURIComponent(date)}`;
  const html = await fetchTransSeeText(url, FALLBACK_TIMEOUT_MS);
  const busNumber = extractBusNumberFromTripSched(html);
  if (!busNumber) return null;
  return fetchLocationForBus(busNumber);
}

async function fetchTransSeeTripFallback(block, trips) {
  const candidates = getTripCandidatesForTransSee(trips);
  for (const trip of candidates) {
    try {
      const bus = await fetchBusFromTransSeeTrip(trip);
      if (bus) {
        return { block, buses: [bus] };
      }
    } catch (_) {
      // Try the next candidate trip.
    }
  }
  return null;
}

async function fetchLiveResult(block) {
  if (pendingByBlock.has(block)) {
    return pendingByBlock.get(block);
  }

  const job = enqueue(async () => {
    const trips = await fetchTripsForBlock(block);
    const buses = await fetchBusesForBlock(block);
    if (buses.length > 0) {
      const results = await Promise.allSettled(buses.map((bus) => fetchLocationForBus(bus)));
      const locations = results
        .filter((result) => result.status === 'fulfilled')
        .map((result) => result.value);
      if (locations.length > 0) {
        return { block, buses: locations };
      }
    }

    const transSeeFallback = await fetchTransSeeTripFallback(block, trips);
    if (transSeeFallback) {
      return transSeeFallback;
    }

    return { block, buses: [] };
  }).finally(() => {
    pendingByBlock.delete(block);
  });

  pendingByBlock.set(block, job);
  return job;
}

async function fetchLiveResultWithFallback(block) {
  try {
    return await withTimeout(fetchLiveResult(block), RUN_TIMEOUT_MS);
  } catch (directErr) {
    if (!ENABLE_PLAYWRIGHT_FALLBACK) throw directErr;
    if (Number(directErr.code) === 400) throw directErr;
    const fallback = await withTimeout(trackBlock(block, { headless: true }), FALLBACK_TIMEOUT_MS);
    return fallback;
  }
}

function drainQueue() {
  while (activeWorkers < TRACK_CONCURRENCY && queue.length > 0) {
    const next = queue.shift();
    activeWorkers += 1;

    next
      .job()
      .then(next.resolve, next.reject)
      .finally(() => {
        activeWorkers -= 1;
        drainQueue();
      });
  }
}

function enqueue(job) {
  return new Promise((resolve, reject) => {
    queue.push({ job, resolve, reject });
    drainQueue();
  });
}

function parseBlockFromReq(req) {
  if (typeof req.query.block === 'string') {
    return normalizeBlock(req.query.block);
  }

  const text = String(req.body?.message || '').trim();
  const match = text.match(/\b(\d{1,3}-\d{1,3})\b/);
  return normalizeBlock(match ? match[1] : text);
}

function validateBlockOrSend(block, res) {
  if (!block) {
    res.status(400).json({ ok: false, error: 'Send a block number like 44-07.' });
    return false;
  }

  if (!isLikelyBlock(block)) {
    res.status(400).json({ ok: false, error: 'Block format must look like 44-07.' });
    return false;
  }

  return true;
}

function formatChatReply(payload) {
  const buses = Array.isArray(payload?.buses) ? payload.buses : [];
  if (!buses.length) {
    return `Block ${payload?.block || ''}: no live data is available right now across the tracking sites either 😉`.trim();
  }

  const lines = [`Block ${payload.block}`];
  for (const bus of buses) {
    lines.push(`Bus ${bus.busNumber}: ${bus.locationText}`);
  }
  return lines.join('\n');
}

async function handleLookup(req, res) {
  const rawBlock = parseBlockFromReq(req);
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const block = await resolveCanonicalBlock(rawBlock);
    if (!block) {
      res.status(404).json({
        ok: false,
        error: `Block not found: ${rawBlock}`,
      });
      return;
    }

    const payload = await fetchLiveResultWithFallback(block);
    res.json({
      ok: true,
      block: payload.block,
      buses: payload.buses,
      cached: false,
      reply: formatChatReply(payload),
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    const code = Number(err.code);
    const status = code === 400 ? 400 : code === 404 ? 404 : code === 504 ? 504 : 500;
    res.status(status).json({
      ok: false,
      error: String(err.message || 'Unexpected error').slice(0, 500),
    });
  }
}

app.get('/api/track', handleLookup);
app.post('/api/chat', handleLookup);

app.get('/healthz', (_req, res) => {
  res.json({
    ok: true,
    uptimeSec: Math.round(process.uptime()),
    queueDepth: queue.length,
    activeWorkers,
    pendingBlocks: pendingByBlock.size,
    liveOnly: true,
    mode: 'direct-http',
  });
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.error(`OC Bus Tracker web app listening on :${PORT}`);
});
