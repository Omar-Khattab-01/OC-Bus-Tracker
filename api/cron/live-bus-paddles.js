'use strict';

const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = String(process.env.SUPABASE_URL || '').trim();
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const CRON_SECRET = String(process.env.CRON_SECRET || '').trim();
const LIVE_BUS_MAPPING_TTL_MS = Number(process.env.LIVE_BUS_MAPPING_TTL_MS || 20 * 60 * 1000);
const CRON_FETCH_CONCURRENCY = Math.max(1, Math.min(6, Number(process.env.CRON_FETCH_CONCURRENCY || 4)));

const adminSupabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
    })
  : null;

let paddleIndexCache = null;

function getOttawaDateParts(date = new Date()) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    weekday: 'long',
  });
  const parts = formatter.formatToParts(date);
  const map = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    weekday: String(map.weekday || '').toLowerCase(),
  };
}

function getServiceDayKeyForDate(date = new Date()) {
  const { weekday, month, day } = getOttawaDateParts(date);
  if (month === 4 && day === 6) return 'easter_monday';
  if (weekday === 'saturday') return 'saturday';
  if (weekday === 'sunday') return 'sunday';
  return 'weekday';
}

function getOttawaNowSeconds() {
  const formatted = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'America/Toronto',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(new Date());
  const match = formatted.match(/^(\d{2}):(\d{2}):(\d{2})$/);
  if (!match) return 0;
  return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]);
}

function timeToSeconds(value) {
  const match = String(value || '').trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return null;
  return Number(match[1]) * 3600 + Number(match[2]) * 60;
}

function paddleIdToBlockLabel(paddleId) {
  const value = String(paddleId || '').trim();
  if (!/^\d{6}$/.test(value)) return value;
  return `${Number(value.slice(0, 3))}-${String(Number(value.slice(3))).padStart(2, '0')}`;
}

function loadPaddleIndex() {
  if (paddleIndexCache) return paddleIndexCache;
  const filePath = path.join(process.cwd(), 'data', 'paddles.index.json');
  paddleIndexCache = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return paddleIndexCache;
}

function buildRunTimeline(run) {
  const timeline = [];
  const trips = Array.isArray(run?.trips) ? run.trips : [];
  let previousStart = null;
  let dayOffset = 0;

  for (const trip of trips) {
    const rawStart = timeToSeconds(trip.start_time);
    const rawEnd = timeToSeconds(trip.end_time);
    if (rawStart === null) continue;
    if (previousStart !== null && rawStart < previousStart) {
      dayOffset += 24 * 3600;
    }
    let start = rawStart + dayOffset;
    let end = (rawEnd !== null ? rawEnd : rawStart) + dayOffset;
    if (end < start) {
      end += 24 * 3600;
    }
    timeline.push({
      tripNumber: trip.trip_number,
      route: String(trip.route || ''),
      headsign: String(trip.headsign || ''),
      startTime: String(trip.start_time || ''),
      endTime: String(trip.end_time || ''),
      start,
      end,
    });
    previousStart = rawStart;
  }

  return timeline;
}

function findActiveTripInRun(run, compareSeconds) {
  const timeline = buildRunTimeline(run);
  return timeline.find((trip) => trip.start <= compareSeconds && compareSeconds <= trip.end + 20 * 60) || null;
}

function getActivePaddlesForNow() {
  const index = loadPaddleIndex();
  const now = new Date();
  const currentSeconds = getOttawaNowSeconds();
  const currentDayKey = getServiceDayKeyForDate(now);
  const previousDate = new Date(now.getTime() - 24 * 3600 * 1000);
  const previousDayKey = getServiceDayKeyForDate(previousDate);

  const results = [];
  const seen = new Set();

  function collectRuns(serviceDay, compareSeconds, requireOvernightCarry = false) {
    const runs = index?.service_days?.[serviceDay] || {};
    for (const [paddleId, run] of Object.entries(runs)) {
      const activeTrip = findActiveTripInRun(run, compareSeconds);
      if (!activeTrip) continue;
      if (requireOvernightCarry && activeTrip.end <= 24 * 3600) continue;
      const block = paddleIdToBlockLabel(paddleId);
      if (seen.has(block)) continue;
      seen.add(block);
      results.push({
        block,
        paddleId,
        serviceDay,
        route: activeTrip.route,
        headsign: activeTrip.headsign,
        tripNumber: activeTrip.tripNumber,
        startTime: activeTrip.startTime,
        endTime: activeTrip.endTime,
      });
    }
  }

  collectRuns(currentDayKey, currentSeconds, false);
  collectRuns(previousDayKey, currentSeconds + 24 * 3600, true);

  return results.sort((a, b) =>
    a.route.localeCompare(b.route, undefined, { numeric: true }) ||
    a.block.localeCompare(b.block, undefined, { numeric: true })
  );
}

function buildOriginBaseUrl(req) {
  const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').trim();
  const proto = String(req.headers['x-forwarded-proto'] || '').trim() || (host.includes('localhost') ? 'http' : 'https');
  if (!host) return '';
  return `${proto}://${host}`;
}

function buildLiveBusPaddleRow(busNumber, value, verifiedAt) {
  return {
    bus_number: String(busNumber || '').trim(),
    block: String(value.block || '').trim(),
    paddle_id: String(value.paddleId || '').trim(),
    service_day: String(value.serviceDay || '').trim(),
    route: String(value.route || '').trim(),
    trip_number: String(value.tripNumber || '').trim(),
    headsign: String(value.headsign || '').trim(),
    start_time: String(value.startTime || '').trim(),
    end_time: String(value.endTime || '').trim(),
    verified_at: verifiedAt,
  };
}

async function fetchLiveBlockPayload(baseUrl, block) {
  const response = await fetch(`${baseUrl}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: block }),
  });
  const data = await response.json().catch(() => null);
  if (!response.ok || !data?.ok) {
    return null;
  }
  return data;
}

async function buildLiveBusPaddleMappings(baseUrl) {
  const activePaddles = getActivePaddlesForNow();
  const mappings = new Map();
  let index = 0;

  async function worker() {
    while (index < activePaddles.length) {
      const currentIndex = index;
      index += 1;
      const activePaddle = activePaddles[currentIndex];
      try {
        const payload = await fetchLiveBlockPayload(baseUrl, activePaddle.block);
        for (const bus of payload?.buses || []) {
          const busNumber = String(bus?.busNumber || '').trim();
          if (!busNumber) continue;
          mappings.set(busNumber, {
            block: activePaddle.block,
            paddleId: activePaddle.paddleId,
            serviceDay: activePaddle.serviceDay,
            route: activePaddle.route,
            headsign: activePaddle.headsign,
            tripNumber: activePaddle.tripNumber,
            startTime: activePaddle.startTime,
            endTime: activePaddle.endTime,
          });
        }
      } catch (_) {
        // Ignore per-block failures so the refresh still completes.
      }
    }
  }

  await Promise.all(Array.from({ length: CRON_FETCH_CONCURRENCY }, () => worker()));
  return { activePaddles, mappings };
}

function isAuthorizedCronRequest(req) {
  const authHeader = String(req.headers.authorization || '').trim();
  if (CRON_SECRET && authHeader === `Bearer ${CRON_SECRET}`) {
    return true;
  }
  if (String(req.headers['x-vercel-cron'] || '').trim() === '1') {
    return true;
  }
  const forwardedFor = String(req.headers['x-forwarded-for'] || '').trim();
  return !forwardedFor || forwardedFor.startsWith('127.0.0.1') || forwardedFor.startsWith('::1');
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') {
    res.status(405).json({ ok: false, error: 'Method not allowed.' });
    return;
  }

  if (!isAuthorizedCronRequest(req)) {
    res.status(401).json({ ok: false, error: 'Unauthorized cron request.' });
    return;
  }

  if (!adminSupabase) {
    res.status(501).json({ ok: false, error: 'Set SUPABASE_SERVICE_ROLE_KEY before refreshing live bus paddle mappings.' });
    return;
  }

  const baseUrl = buildOriginBaseUrl(req);
  if (!baseUrl) {
    res.status(500).json({ ok: false, error: 'Could not determine base URL for cron refresh.' });
    return;
  }

  try {
    const { activePaddles, mappings } = await buildLiveBusPaddleMappings(baseUrl);
    const verifiedAt = new Date().toISOString();
    const rows = Array.from(mappings.entries())
      .map(([busNumber, value]) => buildLiveBusPaddleRow(busNumber, value, verifiedAt))
      .filter((row) => row.bus_number && row.block);

    if (rows.length) {
      const { error: upsertError } = await adminSupabase
        .from('live_bus_paddles')
        .upsert(rows, { onConflict: 'bus_number' });
      if (upsertError) throw upsertError;
    }

    const staleBefore = new Date(Date.now() - LIVE_BUS_MAPPING_TTL_MS).toISOString();
    const { error: deleteError } = await adminSupabase
      .from('live_bus_paddles')
      .delete()
      .lt('verified_at', staleBefore);
    if (deleteError) throw deleteError;

    res.status(200).json({
      ok: true,
      activePaddles: activePaddles.length,
      mappedBuses: rows.length,
      verifiedAt,
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: String(err.message || 'Unexpected error').slice(0, 500),
    });
  }
};
