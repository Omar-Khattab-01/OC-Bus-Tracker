'use strict';

const express = require('express');
const fs = require('fs');
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
let paddleIndexCache = null;

const app = express();
app.use(express.json({ limit: '100kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const SHUTTLE_DEFINITIONS = {
  east_end_weekday: {
    id: 'east_end_weekday',
    route: '898',
    name: 'East End Shuttle',
    sourceLabel: 'Weekday East End Shuttle (898)',
    sourceFile: 'paddles/Shuttles/EastEndShuttle_Weekdays.jpg',
    stops: ['Hurdman', 'Indy Depot', 'Stl Depot', 'MS/F/Belf', 'Hurdman'],
    firstStart: '05:20',
    lastStart: '19:00',
    intervalMinutes: 20,
    offsets: [0, 5, 9, 11, 14],
  },
  east_end_weekend: {
    id: 'east_end_weekend',
    route: '898',
    name: 'East End Shuttle',
    sourceLabel: 'East End Shuttle (Sat/Sun)',
    sourceFile: 'paddles/Shuttles/EastEndShuttle_Sat_Sun.jpg',
    stops: ['Hurdman', 'Indy Depot', 'Stl Depot', 'MS/F/Belf', 'Hurdman'],
    firstStart: '05:20',
    lastStart: '19:00',
    intervalMinutes: 20,
    offsets: [0, 5, 9, 11, 14],
  },
  pinecrest_lincoln_weekday: {
    id: 'pinecrest_lincoln_weekday',
    route: '899',
    name: 'Pinecrest / Lincoln Loop Shuttle',
    sourceLabel: 'Weekday Pinecrest / Lincoln Loop (899)',
    sourceFile: 'paddles/Shuttles/PincrestLincolnLoopShuttle.jpg',
    stops: ['Pinecrest Depot', 'Lincoln Station', 'Lincoln Station', 'Pinecrest Depot'],
    firstStart: '06:55',
    lastStart: '08:55',
    intervalMinutes: 20,
    offsets: [0, 5, 10, 15],
  },
  pinecrest_baseline_weekday: {
    id: 'pinecrest_baseline_weekday',
    route: '899',
    name: 'Pinecrest to Baseline Shuttle',
    sourceLabel: 'Pinecrest to Baseline (899)',
    sourceFile: 'paddles/Shuttles/PinecrestToBaselineShuttle.jpg',
    trips: [
      ['10:00', '10:05', '10:13', '10:15', '10:23', '10:28'],
      ['10:30', '10:35', '10:43', '10:45', '10:53', '10:58'],
      ['11:00', '11:05', '11:13', '11:15', '11:23', '11:28'],
      ['11:30', '11:35', '11:43', '11:45', '11:53', '11:58'],
      ['12:00', '12:05', '12:13', '12:15', '12:23', '12:28'],
      ['12:30', '12:35', '12:43', '12:45', '12:53', '12:58'],
      ['12:59', '13:04', '13:12', '13:13', '13:21', '13:26'],
      ['13:30', '13:35', '13:43', '13:45', '13:53', '13:58'],
      ['14:01', '14:06', '14:14', '14:15', '14:23', '14:28'],
      ['14:30', '14:35', '14:43', '14:45', '14:53', '14:58'],
      ['15:00', '15:05', '15:13', '15:15', '15:23', '15:28'],
      ['15:30', '15:35', '15:43', '15:45', '15:53', '15:58'],
      ['16:00', '16:05', '16:13', '16:15', '16:23', '16:28'],
      ['16:30', '16:35', '16:43', '16:45', '16:53', '16:58'],
      ['17:00', '17:05', '17:13', '17:15', '17:23', '17:28'],
      ['17:29', '17:34', '17:42', '17:43', '17:51', '17:56'],
      ['18:00', '18:05', '18:13', '18:15', '18:23', '18:28'],
      ['18:31', '18:36', '18:44', '18:45', '18:53', '18:58'],
      ['19:00', '19:05', '19:13', '19:15', '19:23', '19:28'],
    ],
    stops: ['Pinecrest Depot', 'Lincoln Station', 'Baseline Station', 'Baseline Station', 'Lincoln Station', 'Pinecrest Depot'],
  },
  merivale_baseline_weekday: {
    id: 'merivale_baseline_weekday',
    route: '899',
    name: 'Merivale to Baseline Shuttle',
    sourceLabel: 'Merivale to Baseline (899) Weekdays',
    sourceFile: 'paddles/Shuttles/MerivaleToBaselineShuttle_Weekdays.jpg',
    trips: [
      [null, null, '04:50', '05:01'],
      [null, null, '05:05', '05:16'],
      [null, null, '05:34', '05:45'],
      [null, null, '06:04', '06:15'],
      ['10:00', '10:13', '10:15', '10:28'],
      ['10:30', '10:43', '10:45', '10:58'],
      ['11:00', '11:13', '11:15', '11:28'],
      ['11:30', '11:43', '11:45', '11:58'],
      ['11:59', '12:12', '12:13', '12:26'],
      ['12:30', '12:43', '12:45', '12:58'],
      ['13:01', '13:14', '13:15', '13:28'],
      ['13:30', '13:43', '13:45', '13:58'],
      ['14:00', '14:13', '14:15', '14:28'],
      ['14:30', '14:43', '14:45', '14:58'],
      ['15:00', '15:13', '15:15', '15:28'],
      ['15:30', '15:43', '15:45', '15:58'],
      ['16:00', '16:13', '16:15', '16:28'],
      ['16:29', '16:42', '16:43', '16:56'],
      ['17:00', '17:13', '17:15', '17:28'],
      ['17:31', '17:44', '17:45', '17:58'],
      ['18:00', '18:13', '18:15', '18:28'],
      ['18:30', '18:43', '18:45', '18:58'],
      ['19:00', '19:13', '19:15', '19:28'],
    ],
    stops: ['Merivale Depot', 'Baseline Station', 'Baseline Station', 'Merivale Depot'],
  },
  merivale_baseline_weekend: {
    id: 'merivale_baseline_weekend',
    route: '899',
    name: 'Merivale to Baseline Shuttle',
    sourceLabel: 'Sat/Sun Merivale to Baseline (899)',
    sourceFile: 'paddles/Shuttles/MerivaleToBaselineShuttle_SatSun.jpg',
    trips: [
      [null, null, '04:50', '05:01'],
      [null, null, '05:05', '05:16'],
      [null, null, '05:34', '05:45'],
      [null, null, '06:04', '06:15'],
      ['07:00', '07:13', '07:15', '07:28'],
      ['07:30', '07:43', '07:45', '07:58'],
      ['08:00', '08:13', '08:15', '08:28'],
      ['08:30', '08:43', '08:45', '08:58'],
      ['09:00', '09:13', '09:15', '09:28'],
      ['09:30', '09:43', '09:45', '09:58'],
      ['10:00', '10:13', '10:15', '10:28'],
      ['10:30', '10:43', '10:45', '10:58'],
      ['11:00', '11:13', '11:15', '11:28'],
      ['11:30', '11:43', '11:45', '11:58'],
      ['11:59', '12:12', '12:14', '12:27'],
      ['12:30', '12:43', '12:45', '12:58'],
      ['13:02', '13:15', '13:16', '13:29'],
      ['13:30', '13:43', '13:45', '13:58'],
      ['14:00', '14:13', '14:15', '14:28'],
      ['14:30', '14:43', '14:45', '14:58'],
      ['15:00', '15:13', '15:15', '15:28'],
      ['15:30', '15:43', '15:45', '15:58'],
      ['16:00', '16:13', '16:15', '16:28'],
      ['16:29', '16:42', '16:44', '16:57'],
      ['17:00', '17:13', '17:15', '17:28'],
      ['17:32', '17:45', '17:46', '17:59'],
      ['18:00', '18:13', '18:15', '18:28'],
      ['18:30', '18:43', '18:45', '18:58'],
      ['19:00', '19:13', '19:15', '19:28'],
    ],
    stops: ['Merivale Depot', 'Baseline Station', 'Baseline Station', 'Merivale Depot'],
  },
  merivale_billings_weekday: {
    id: 'merivale_billings_weekday',
    route: '899',
    name: 'Merivale to Billings Shuttle',
    sourceLabel: 'Merivale to Billings (899) Weekdays',
    sourceFile: 'paddles/Shuttles/MerivaleToBillingsShuttle_Weekdays.jpg',
    trips: [
      ['10:00', '10:13', '10:15', '10:28'],
      ['10:30', '10:43', '10:45', '10:58'],
      ['11:00', '11:13', '11:15', '11:28'],
      ['11:29', '11:42', '11:43', '11:56'],
      ['12:00', '12:13', '12:15', '12:28'],
      ['12:31', '12:44', '12:45', '12:58'],
      ['13:00', '13:13', '13:15', '13:28'],
      ['13:30', '13:43', '13:45', '13:58'],
      ['14:00', '14:13', '14:15', '14:28'],
      ['14:30', '14:43', '14:45', '14:58'],
      ['15:00', '15:13', '15:15', '15:28'],
      ['15:30', '15:43', '15:45', '15:58'],
      ['15:59', '16:12', '16:13', '16:26'],
      ['16:30', '16:43', '16:45', '16:58'],
      ['17:01', '17:14', '17:15', '17:28'],
      ['17:30', '17:43', '17:45', '17:58'],
      ['18:00', '18:13', '18:15', '18:28'],
      ['18:30', '18:43', '18:45', '18:58'],
      ['19:00', '19:13', '19:15', '19:28'],
    ],
    stops: ['Merivale Depot', 'Billings Bridge Terminal', 'Billings Bridge Terminal', 'Merivale Depot'],
  },
  merivale_billings_sunday: {
    id: 'merivale_billings_sunday',
    route: '899',
    name: 'Merivale to Billings Shuttle',
    sourceLabel: 'Sunday Merivale to Billings (899)',
    sourceFile: 'paddles/Shuttles/MerivaleToBillingsShuttle_Sun.jpg',
    trips: [
      ['10:00', '10:13', '10:15', '10:28'],
      ['10:30', '10:43', '10:45', '10:58'],
      ['11:00', '11:13', '11:15', '11:28'],
      ['11:29', '11:42', '11:44', '11:57'],
      ['12:00', '12:13', '12:15', '12:28'],
      ['12:32', '12:45', '12:46', '12:59'],
      ['13:00', '13:13', '13:15', '13:28'],
      ['13:30', '13:43', '13:45', '13:58'],
      ['14:00', '14:13', '14:15', '14:28'],
      ['14:30', '14:43', '14:45', '14:58'],
      ['15:00', '15:13', '15:15', '15:28'],
      ['15:30', '15:43', '15:45', '15:58'],
      ['15:59', '16:12', '16:14', '16:27'],
      ['16:30', '16:43', '16:45', '16:58'],
      ['17:02', '17:15', '17:16', '17:29'],
      ['17:30', '17:43', '17:45', '17:58'],
      ['18:00', '18:13', '18:15', '18:28'],
      ['18:30', '18:43', '18:45', '18:58'],
      ['19:00', '19:13', '19:15', '19:28'],
    ],
    stops: ['Merivale Depot', 'Billings Bridge Terminal', 'Billings Bridge Terminal', 'Merivale Depot'],
  },
};

const SHUTTLES_BY_SERVICE_DAY = {
  weekday: [
    'east_end_weekday',
    'pinecrest_lincoln_weekday',
    'pinecrest_baseline_weekday',
    'merivale_baseline_weekday',
    'merivale_billings_weekday',
  ],
  easter_monday: [
    'east_end_weekday',
    'pinecrest_lincoln_weekday',
    'pinecrest_baseline_weekday',
    'merivale_baseline_weekday',
    'merivale_billings_weekday',
  ],
  saturday: [
    'east_end_weekend',
    'merivale_baseline_weekend',
  ],
  sunday: [
    'east_end_weekend',
    'merivale_baseline_weekend',
    'merivale_billings_sunday',
  ],
};

function normalizeBlock(input) {
  return String(input || '').trim().toUpperCase();
}

function normalizeMessage(input) {
  return String(input || '').trim();
}

function isLikelyBlock(block) {
  return /^[0-9]{1,3}-[0-9]{1,3}$/.test(block);
}

function isShuttleRequest(text) {
  return /\bshuttles?\b/i.test(String(text || ''));
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

function httpGet(url, timeoutMs = RUN_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, (res) => {
      const chunks = [];
      res.on('data', (chunk) => {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });
      res.on('end', () => {
        const body = Buffer.concat(chunks);
        if (res.statusCode >= 400) {
          const err = new Error(`HTTP ${res.statusCode} for ${url}`);
          err.code = res.statusCode;
          err.body = body.toString('utf8');
          reject(err);
          return;
        }
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body,
          text: body.toString('utf8'),
        });
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Request timeout for ${url}`));
    });

    req.on('error', reject);
  });
}

async function httpGetText(url, timeoutMs = RUN_TIMEOUT_MS) {
  const response = await httpGet(url, timeoutMs);
  return response.text;
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

function getOttawaServiceDayKey() {
  const now = new Date();
  const ottawaIso = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
  if (ottawaIso === '2026-04-06') {
    return 'easter_monday';
  }

  const weekday = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Toronto',
    weekday: 'long',
  }).format(new Date()).toLowerCase();

  if (weekday === 'saturday') return 'saturday';
  if (weekday === 'sunday') return 'sunday';
  return 'weekday';
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

function secondsToTime(value) {
  if (!Number.isFinite(value)) return '';
  const total = Math.max(0, Math.trunc(value));
  const hh = Math.floor(total / 3600);
  const mm = Math.floor((total % 3600) / 60);
  return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
}

function buildPatternTrips(definition) {
  const firstStart = timeToSeconds(definition.firstStart);
  const lastStart = timeToSeconds(definition.lastStart);
  const interval = Number(definition.intervalMinutes || 0) * 60;
  if (firstStart === null || lastStart === null || !interval) return [];

  const trips = [];
  let tripNumber = 1;

  for (let start = firstStart; start <= lastStart; start += interval) {
    const stops = definition.stops.map((stop, index) => ({
      name: stop,
      time: secondsToTime(start + Number(definition.offsets[index] || 0) * 60),
    }));
    trips.push({
      tripNumber,
      startTime: stops[0]?.time || '',
      endTime: stops[stops.length - 1]?.time || '',
      stops,
    });
    tripNumber += 1;
  }

  return trips;
}

function buildExplicitTrips(definition) {
  const trips = Array.isArray(definition.trips) ? definition.trips : [];
  const stops = Array.isArray(definition.stops) ? definition.stops : [];
  return trips.map((times, index) => {
    const stopEntries = stops.map((stop, stopIndex) => ({
      name: stop,
      time: String(times[stopIndex] || '').trim(),
    })).filter((entry) => entry.time);

    return {
      tripNumber: index + 1,
      startTime: stopEntries[0]?.time || '',
      endTime: stopEntries[stopEntries.length - 1]?.time || '',
      stops: stopEntries,
    };
  }).filter((trip) => trip.stops.length > 0);
}

function getShuttleForToday(id) {
  const definition = SHUTTLE_DEFINITIONS[id];
  if (!definition) return null;

  return {
    ...definition,
    trips: Array.isArray(definition.trips)
      ? buildExplicitTrips(definition)
      : buildPatternTrips(definition),
  };
}

function getAvailableShuttlesForDay(serviceDay = getOttawaServiceDayKey()) {
  const ids = SHUTTLES_BY_SERVICE_DAY[serviceDay] || [];
  return ids
    .map((id) => getShuttleForToday(id))
    .filter(Boolean);
}

function describeNextShuttleStop(shuttle) {
  const nowSeconds = timeToSeconds(
    new Intl.DateTimeFormat('en-GB', {
      timeZone: 'America/Toronto',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date())
  ) ?? 0;

  const flattened = [];
  for (const trip of shuttle.trips || []) {
    for (let index = 0; index < (trip.stops || []).length; index += 1) {
      const stop = trip.stops[index];
      const stopSeconds = timeToSeconds(stop.time);
      if (stopSeconds === null) continue;
      flattened.push({
        tripNumber: trip.tripNumber,
        stopIndex: index,
        stopName: stop.name,
        stopTime: stop.time,
        stopSeconds,
      });
    }
  }

  if (!flattened.length) {
    return {
      summary: 'No shuttle times are available.',
      nextStopName: null,
      nextStopTime: null,
      currentSegment: null,
    };
  }

  const next = flattened.find((entry) => entry.stopSeconds >= nowSeconds);
  if (!next) {
    return {
      summary: 'No more shuttle trips are scheduled for today.',
      nextStopName: null,
      nextStopTime: null,
      currentSegment: null,
    };
  }

  const previous = [...flattened].reverse().find((entry) => entry.stopSeconds < nowSeconds) || null;
  const currentSegment = previous && previous.tripNumber === next.tripNumber
    ? `Between ${previous.stopName} (${previous.stopTime}) and ${next.stopName} (${next.stopTime}) on trip ${next.tripNumber}`
    : `Next trip is trip ${next.tripNumber}`;

  return {
    summary: `Next stop: ${next.stopName} at ${next.stopTime}.`,
    nextStopName: next.stopName,
    nextStopTime: next.stopTime,
    currentSegment,
  };
}

function buildShuttleResponse(id) {
  const serviceDay = getOttawaServiceDayKey();
  const availableIds = SHUTTLES_BY_SERVICE_DAY[serviceDay] || [];
  if (!availableIds.includes(id)) return null;

  const shuttle = getShuttleForToday(id);
  if (!shuttle) return null;
  const nextStop = describeNextShuttleStop(shuttle);

  return {
    ok: true,
    id: shuttle.id,
    route: shuttle.route,
    name: shuttle.name,
    serviceDay,
    sourceLabel: shuttle.sourceLabel,
    sourceFile: shuttle.sourceFile,
    stops: shuttle.stops,
    trips: shuttle.trips,
    nextStop,
  };
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

function loadPaddleIndex() {
  if (!paddleIndexCache) {
    const filePath = path.join(__dirname, 'data', 'paddles.index.json');
    paddleIndexCache = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  }
  return paddleIndexCache;
}

function blockToPaddleId(block) {
  const match = String(block || '').trim().toUpperCase().match(/^([A-Z0-9]+)-(\d{1,3})$/);
  if (!match) return null;
  return `${match[1].padStart(3, '0')}${match[2].padStart(3, '0')}`;
}

async function fetchPaddleTripsForBlock(block) {
  const paddleId = blockToPaddleId(block);
  if (!paddleId) return [];

  const serviceDay = getOttawaServiceDayKey();
  const run = getPaddleRunForDay(serviceDay, paddleId);
  if (!run || !Array.isArray(run.trips)) {
    return [];
  }

  return run.trips.map((trip) => ({
    tripId: null,
    sourceType: 'paddle',
    routeId: String(trip.route || ''),
    headSign: String(trip.headsign || ''),
    routeDirection: 0,
    scheduledStartTime: String(trip.start_time || ''),
    scheduledEndTime: String(trip.end_time || ''),
    actualStartTime: null,
    actualEndTime: null,
    delay: null,
    canceled: null,
    busId: null,
    startStop: String(trip.start_stop || ''),
    endStop: String(trip.end_stop || ''),
    paddleId,
    sourceId: run.source_id,
  })).filter((trip) => trip.routeId && trip.scheduledStartTime);
}

function getPaddleRunForDay(serviceDay, paddleId) {
  const index = loadPaddleIndex();
  return index?.service_days?.[serviceDay]?.[paddleId] || null;
}

function buildPaddleResponse(block) {
  const paddleId = blockToPaddleId(block);
  if (!paddleId) return null;

  const serviceDay = getOttawaServiceDayKey();
  const run = getPaddleRunForDay(serviceDay, paddleId);
  if (!run) return null;

  return {
    block: String(block),
    paddleId,
    serviceDay,
    sourceId: run.source_id || null,
    sourceLabel: run.source_label || null,
    effective: run.effective || null,
    garage: run.garage || null,
    signOn: run.sign_on || null,
    routes: Array.isArray(run.routes) ? run.routes : [],
    busType: run.bus_type || null,
    trips: Array.isArray(run.trips) ? run.trips : [],
  };
}

function getBestPaddleTripCandidates(trips) {
  const nowSeconds = timeToSeconds(
    new Intl.DateTimeFormat('en-GB', {
      timeZone: 'America/Toronto',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date())
  ) ?? 0;

  const scheduled = [...trips]
    .filter((trip) => trip && trip.sourceType === 'paddle')
    .map((trip) => ({
      trip,
      start: timeToSeconds(trip.scheduledStartTime),
      end: timeToSeconds(trip.scheduledEndTime),
    }))
    .filter((entry) => entry.start !== null)
    .sort((a, b) => a.start - b.start)
    .map((entry, index, arr) => ({ ...entry, index, total: arr.length }));

  if (!scheduled.length) return [];

  const currentIndex = scheduled.findIndex((entry) =>
    entry.end !== null
      ? entry.start <= nowSeconds && nowSeconds <= entry.end + 20 * 60
      : Math.abs(nowSeconds - entry.start) <= 20 * 60
  );
  const seen = new Set();
  const candidates = [];

  function push(entry) {
    if (!entry || seen.has(entry.index)) return;
    seen.add(entry.index);
    candidates.push(entry.trip);
  }

  if (currentIndex >= 0) {
    push(scheduled[currentIndex]);

    const previous = scheduled[currentIndex - 1];
    if (previous) {
      const previousEnd = previous.end ?? previous.start;
      if (previousEnd !== null && nowSeconds - previousEnd <= 45 * 60) {
        push(previous);
      }
    }

    const next = scheduled[currentIndex + 1];
    if (next && next.start - nowSeconds <= 20 * 60) {
      push(next);
    }

    push(scheduled[currentIndex - 2]);
    push(scheduled[currentIndex + 2]);
    return candidates;
  }

  const nextIndex = scheduled.findIndex((entry) => entry.start > nowSeconds);
  if (nextIndex >= 0) {
    push(scheduled[nextIndex]);
    push(scheduled[nextIndex - 1]);
    push(scheduled[nextIndex + 1]);
    push(scheduled[nextIndex - 2]);
    return candidates;
  }

  push(scheduled[scheduled.length - 1]);
  push(scheduled[scheduled.length - 2]);
  return candidates;
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

function normalizeHeadsign(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&[^;\s]+;/g, ' ')
    .replace(/station/g, 'stn')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizeStopLabel(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&[^;\s]+;/g, ' ')
    .replace(/\bst[.-]?\b/g, 'st ')
    .replace(/\bstation\b/g, ' ')
    .replace(/\bstn\b/g, ' ')
    .replace(/\bbus stop\b/g, ' ')
    .replace(/\baeroport\b/g, 'airport')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function transSeeTimeTo24Hour(value) {
  const match = String(value || '').trim().match(/^(\d{1,2}):(\d{2}):(\d{2})(AM|PM)$/i);
  if (!match) return null;
  let hour = Number(match[1]);
  const minute = match[2];
  if (match[4].toUpperCase() === 'PM' && hour !== 12) hour += 12;
  if (match[4].toUpperCase() === 'AM' && hour === 12) hour = 0;
  return `${String(hour).padStart(2, '0')}:${minute}`;
}

function stripTags(value) {
  return decodeEntities(String(value || '').replace(/<[^>]+>/g, ' ')).replace(/\s+/g, ' ').trim();
}

async function fetchTripIdFromTransSeeRouteSchedule(trip) {
  const route = String(trip.routeId || '').trim();
  if (!route) return null;

  const url = `https://www.transsee.ca/routesched?a=octranspo&r=${encodeURIComponent(route)}&date=${encodeURIComponent(getOttawaServiceDateString())}`;
  const html = await fetchTransSeeText(url, FALLBACK_TIMEOUT_MS);

  const targetStart = String(trip.scheduledStartTime || '').slice(0, 5);
  const targetEnd = String(trip.scheduledEndTime || '').slice(0, 5);
  const targetHeadsign = normalizeHeadsign(trip.headSign);
  const targetStartStop = normalizeStopLabel(trip.startStop);
  const targetEndStop = normalizeStopLabel(trip.endStop);
  const targetStartSeconds = timeToSeconds(targetStart);
  const targetEndSeconds = timeToSeconds(targetEnd);

  const rowRegex = /<tr[^>]*><td><a href="tripsched\?a=octranspo&t=([^"&]+)&date=[^"]+">([\s\S]*?)<\/a><\/td><td>([\s\S]*?)<\/td><td>([\d:APM]+)<\/td><td>([\s\S]*?)<\/td><td>([\d:APM]+)<\/td><td>([\d:]+)<\/td><\/tr>/gi;
  let best = null;
  let match;

  while ((match = rowRegex.exec(html))) {
    const tripId = match[1];
    const headsign = stripTags(match[2]);
    const startStop = stripTags(match[3]);
    const startTime = transSeeTimeTo24Hour(match[4]);
    const endStop = stripTags(match[5]);
    const endTime = transSeeTimeTo24Hour(match[6]);
    const startSeconds = timeToSeconds(startTime);
    const endSeconds = timeToSeconds(endTime);
    let score = 0;

    if (startTime && startTime === targetStart) score += 5;
    if (endTime && endTime === targetEnd) score += 4;
    if (targetStartStop && normalizeStopLabel(startStop) === targetStartStop) score += 4;
    if (targetEndStop && normalizeStopLabel(endStop) === targetEndStop) score += 4;
    if (normalizeHeadsign(headsign) === targetHeadsign) score += 3;
    if (normalizeHeadsign(headsign).includes(targetHeadsign) || targetHeadsign.includes(normalizeHeadsign(headsign))) score += 1;
    if (targetStartSeconds !== null && startSeconds !== null) {
      const diff = Math.abs(startSeconds - targetStartSeconds);
      if (diff <= 5 * 60) score += 4;
      else if (diff <= 10 * 60) score += 2;
    }
    if (targetEndSeconds !== null && endSeconds !== null) {
      const diff = Math.abs(endSeconds - targetEndSeconds);
      if (diff <= 5 * 60) score += 3;
      else if (diff <= 10 * 60) score += 1;
    }

    if (score > 0 && (!best || score > best.score)) {
      best = { tripId, score };
    }
  }

  return best ? best.tripId : null;
}

function getTripCandidatePriority(trip) {
  const nowSeconds = timeToSeconds(
    new Intl.DateTimeFormat('en-GB', {
      timeZone: 'America/Toronto',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date())
  ) ?? 0;
  const actualStart = timeToSeconds(trip.actualStartTime);
  const actualEnd = timeToSeconds(trip.actualEndTime);
  const scheduledStart = timeToSeconds(trip.scheduledStartTime);
  const scheduledEnd = timeToSeconds(trip.scheduledEndTime);

  if (actualStart !== null && actualEnd === null) {
    return 300000 + actualStart;
  }
  if (actualStart !== null) {
    return 200000 + actualStart;
  }
  if (scheduledStart !== null && scheduledEnd !== null && scheduledStart <= nowSeconds && nowSeconds <= scheduledEnd + 20 * 60) {
    return 150000 + scheduledStart;
  }
  if (scheduledStart !== null) {
    return 100000 + scheduledStart;
  }
  return 0;
}

function getTripCandidatesForTransSee(trips) {
  const paddleCandidates = getBestPaddleTripCandidates(trips);
  if (paddleCandidates.length > 0) {
    return paddleCandidates;
  }

  return [...trips]
    .filter((trip) => trip && trip.routeId && (trip.tripId || trip.scheduledStartTime))
    .sort((a, b) => getTripCandidatePriority(b) - getTripCandidatePriority(a))
    .slice(0, 5);
}

function extractBusNumberFromTripSched(html) {
  const blockSectionMatch = html.match(/<div id=block><h4>Trips in this block<\/h4>([\s\S]*?)<\/table><\/div>/i);
  if (!blockSectionMatch) return null;

  const section = blockSectionMatch[1];
  const currentRowMatch = section.match(/<tr><td>[\s\S]*?<td style="font-weight:\s*bold;">[\s\S]*?<\/td><td[\s\S]*?<\/td><td[\s\S]*?<\/td><td[\s\S]*?<\/td><td>([\s\S]*?)<\/td><\/tr>/i);
  if (!currentRowMatch) return null;

  const tripPathCell = currentRowMatch[1];
  const busMatch = tripPathCell.match(/>(\d{3,5})<\/a>/);
  return busMatch ? busMatch[1] : null;
}

async function fetchBusFromTransSeeTrip(trip) {
  const date = getOttawaServiceDateString();
  const tripId = trip.tripId || await fetchTripIdFromTransSeeRouteSchedule(trip);
  if (!tripId) return null;

  const url = `https://www.transsee.ca/tripsched?a=octranspo&t=${encodeURIComponent(tripId)}&date=${encodeURIComponent(date)}`;
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
    let trips = [];
    let directLookupError = null;

    try {
      trips = await fetchTripsForBlock(block);
    } catch (err) {
      directLookupError = err;
    }

    if (!trips.length) {
      trips = await fetchPaddleTripsForBlock(block);
    }

    let buses = [];
    if (!directLookupError) {
      try {
        buses = await fetchBusesForBlock(block);
      } catch (err) {
        directLookupError = err;
      }
    }
    if (buses.length > 0) {
      const results = await Promise.allSettled(buses.map((bus) => fetchLocationForBus(bus)));
      const locations = results
        .filter((result) => result.status === 'fulfilled')
        .map((result) => result.value);
      if (locations.length > 0) {
        return { block, buses: locations };
      }
    }

    if (trips.length > 0) {
      const transSeeFallback = await fetchTransSeeTripFallback(block, trips);
      if (transSeeFallback) {
        return transSeeFallback;
      }
    } else if (directLookupError) {
      throw directLookupError;
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

function parseMessageText(req) {
  if (typeof req.body?.message === 'string') {
    return normalizeMessage(req.body.message);
  }
  if (typeof req.query?.message === 'string') {
    return normalizeMessage(req.query.message);
  }
  return '';
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
    return `Block ${payload?.block || ''}: no live data is available right now across the tracking sites either \u{1F609}`.trim();
  }

  const lines = [`Block ${payload.block}`];
  for (const bus of buses) {
    lines.push(`Bus ${bus.busNumber}: ${bus.locationText}`);
  }
  return lines.join('\n');
}

function formatShuttleListReply(serviceDay, shuttles) {
  const label = serviceDay.replace(/_/g, ' ');
  if (!shuttles.length) {
    return `No worker shuttles are listed for ${label}.`;
  }
  const lines = [`Available worker shuttles for ${label}:`];
  for (const shuttle of shuttles) {
    lines.push(`${shuttle.route}: ${shuttle.name}`);
  }
  lines.push('Tap a shuttle below to view the schedule and next stop.');
  return lines.join('\n');
}

async function handleLookup(req, res) {
  const rawBlock = parseBlockFromReq(req);
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const canonicalBlock = await resolveCanonicalBlock(rawBlock);
    if (!canonicalBlock && !blockToPaddleId(rawBlock)) {
      res.status(404).json({
        ok: false,
        error: `Block not found: ${rawBlock}`,
      });
      return;
    }

    const block = canonicalBlock || rawBlock;
    const payload = await fetchLiveResultWithFallback(block);
    const paddle = buildPaddleResponse(block);
    res.json({
      ok: true,
      block: payload.block,
      buses: payload.buses,
      paddleAvailable: Boolean(paddle),
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

async function handleChat(req, res) {
  const message = parseMessageText(req);
  if (isShuttleRequest(message)) {
    const serviceDay = getOttawaServiceDayKey();
    const shuttles = getAvailableShuttlesForDay(serviceDay).map((shuttle) => ({
      id: shuttle.id,
      route: shuttle.route,
      name: shuttle.name,
      nextStop: describeNextShuttleStop(shuttle),
    }));

    res.json({
      ok: true,
      mode: 'shuttle-list',
      reply: formatShuttleListReply(serviceDay, shuttles),
      shuttleOptions: shuttles,
      generatedAt: new Date().toISOString(),
    });
    return;
  }

  return handleLookup(req, res);
}

async function handlePaddle(req, res) {
  const rawBlock = parseBlockFromReq(req);
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const canonicalBlock = await resolveCanonicalBlock(rawBlock);
    const block = canonicalBlock || rawBlock;
    const paddle = buildPaddleResponse(block);
    if (!paddle) {
      res.status(404).json({
        ok: false,
        error: `Paddle not found for ${block}`,
      });
      return;
    }

    res.json({
      ok: true,
      ...paddle,
    });
  } catch (err) {
    const code = Number(err.code);
    const status = code === 400 ? 400 : code === 404 ? 404 : 500;
    res.status(status).json({
      ok: false,
      error: String(err.message || 'Unexpected error').slice(0, 500),
    });
  }
}

async function handleShuttle(req, res) {
  const shuttleId = normalizeMessage(req.query.id || req.query.shuttle);
  if (!shuttleId) {
    res.status(400).json({
      ok: false,
      error: 'Choose a shuttle first.',
    });
    return;
  }

  const shuttle = buildShuttleResponse(shuttleId);
  if (!shuttle) {
    res.status(404).json({
      ok: false,
      error: `Shuttle not found: ${shuttleId}`,
    });
    return;
  }

  res.json(shuttle);
}

app.get('/api/track', handleLookup);
app.post('/api/chat', handleChat);
app.get('/api/paddle', handlePaddle);
app.get('/api/shuttle', handleShuttle);

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
