'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const { trackBlock } = require('./track_block');
const {
  debugGtfsState,
  getGtfsWarmupStatus,
  isConfigured: isGtfsRtConfigured,
  lookupBlockWithGtfsRt,
  lookupBusPositionWithGtfsRt,
  lookupBusWithGtfsRt,
  warmGtfsRtCaches,
} = require('./lib/gtfs_rt_runtime');

const PORT = Number(process.env.PORT || 7860);
const RUN_TIMEOUT_MS = Number(process.env.RUN_TIMEOUT_MS || 25000);
const FALLBACK_TIMEOUT_MS = Number(process.env.FALLBACK_TIMEOUT_MS || 90000);
const TRACK_CONCURRENCY = Math.max(1, Number(process.env.TRACK_CONCURRENCY || 6));
const ENABLE_PLAYWRIGHT_FALLBACK = process.env.ENABLE_PLAYWRIGHT_FALLBACK === '1';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').trim();
const SUPABASE_ANON_KEY = String(process.env.SUPABASE_ANON_KEY || '').trim();
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const CRON_SECRET = String(process.env.CRON_SECRET || '').trim();
const LIVE_BUS_MAPPING_TTL_MS = Number(process.env.LIVE_BUS_MAPPING_TTL_MS || 20 * 60 * 1000);
const APRIL19_PADDLE_SWITCH_DATE = '2026-04-19';

const pendingByBlock = new Map();
const queue = [];
let activeWorkers = 0;
let paddleIndexCache = null;
const busBlockCache = new Map();
const liveBusPaddleCache = new Map();
let liveBusPaddleRefreshPromise = null;

const adminSupabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
    })
  : null;

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

const SHUTTLE_DAY_OPTIONS = ['weekday', 'saturday', 'sunday'];

function normalizeBlock(input) {
  return String(input || '').trim().toUpperCase();
}

function normalizeMessage(input) {
  return String(input || '').trim();
}

function isLikelyBlock(block) {
  return /^[0-9]{1,3}-[0-9]{1,3}$/.test(block);
}

function isLikelyBusNumber(value) {
  return /^\d{3,5}$/.test(String(value || '').trim());
}

function isShuttleRequest(text) {
  return /\bshuttles?\b/i.test(String(text || ''));
}

function normalizeServiceDay(input) {
  const value = String(input || '').trim().toLowerCase();
  if (!value) return '';
  if (value === 'today') return getOttawaServiceDayKey();
  if (value === 'weekdays' || value === 'weekday') return 'weekday';
  if (value === 'saturday' || value === 'sat') return 'saturday';
  if (value === 'sunday' || value === 'sun') return 'sunday';
  if (value === 'easter monday' || value === 'easter_monday') return 'easter_monday';
  return '';
}

function formatServiceDayLabel(day) {
  const value = String(day || '').replace(/_/g, ' ').trim();
  if (!value) return 'Today';
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function parseRequestedShuttleDay(text) {
  const value = String(text || '').trim().toLowerCase();
  if (!value) return '';
  const match = value.match(/\b(today|weekday|weekdays|saturday|sat|sunday|sun|easter monday)\b/);
  return normalizeServiceDay(match ? match[1] : '');
}

function isShowAllRequest(text) {
  return /^showall$/i.test(String(text || '').trim());
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

function getOttawaDateString(date = new Date()) {
  const ottawaIso = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
  return ottawaIso;
}

function getServiceDayKeyForDate(date = new Date()) {
  const ottawaIso = getOttawaDateString(date);
  if (ottawaIso === '2026-04-06') {
    return 'easter_monday';
  }

  const weekday = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Toronto',
    weekday: 'long',
  }).format(date).toLowerCase();

  if (weekday === 'saturday') return 'saturday';
  if (weekday === 'sunday') return 'sunday';
  return 'weekday';
}

function getOttawaServiceDayKey() {
  return getServiceDayKeyForDate(new Date());
}

function isEasterMondayOptionVisible(referenceDate = new Date()) {
  const ottawaIso = getOttawaDateString(referenceDate);
  return ottawaIso === '2026-04-05' || ottawaIso === '2026-04-06';
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

async function fetchTripsForResolvedBlock(block) {
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

function findEquivalentShuttleForDay(id, serviceDay) {
  const source = SHUTTLE_DEFINITIONS[id];
  if (!source) return null;
  const candidates = getAvailableShuttlesForDay(serviceDay);
  return candidates.find((candidate) =>
    candidate.id !== id &&
    candidate.route === source.route &&
    candidate.name === source.name
  ) || null;
}

function getShuttleServiceLabel(shuttleId) {
  if (/_weekday$/i.test(shuttleId)) return 'Weekday';
  if (/_weekend$/i.test(shuttleId)) return 'Saturday/Sunday';
  if (/_sunday$/i.test(shuttleId)) return 'Sunday';
  if (/_saturday$/i.test(shuttleId)) return 'Saturday';
  return '';
}

function paddleIdToBlockLabel(paddleId) {
  const text = String(paddleId || '').trim();
  const match = text.match(/^([A-Z0-9]{3})(\d{3})$/);
  if (!match) return text;
  return `${String(Number(match[1]))}-${String(Number(match[2]))}`;
}

function getAccountBlockOptions() {
  const index = loadPaddleIndex();
  const result = {
    weekday: [],
    saturday: [],
    sunday: [],
  };

  for (const serviceDay of Object.keys(result)) {
    const runs = index?.service_days?.[serviceDay] || {};
    result[serviceDay] = Object.keys(runs)
      .map((paddleId) => paddleIdToBlockLabel(paddleId))
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  }

  return result;
}

function getAccountShuttleOptions() {
  return Object.values(SHUTTLE_DEFINITIONS)
    .map((shuttle) => ({
      id: shuttle.id,
      route: shuttle.route,
      name: shuttle.name,
      label: `${shuttle.route} ${shuttle.name}${getShuttleServiceLabel(shuttle.id) ? ` (${getShuttleServiceLabel(shuttle.id)})` : ''}`,
    }))
    .sort((a, b) =>
      a.route.localeCompare(b.route, undefined, { numeric: true }) ||
      a.name.localeCompare(b.name, undefined, { numeric: true })
    );
}

function getOttawaNowSeconds() {
  return timeToSeconds(
    new Intl.DateTimeFormat('en-GB', {
      timeZone: 'America/Toronto',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date())
  ) ?? 0;
}

function buildRunTimeline(run) {
  const trips = Array.isArray(run?.trips) ? run.trips : [];
  const timeline = [];
  let dayOffset = 0;
  let previousStart = null;

  for (const trip of trips) {
    const rawStart = timeToSeconds(trip.start_time);
    const rawEnd = timeToSeconds(trip.end_time);
    if (rawStart === null) continue;

    if (previousStart !== null && rawStart < previousStart) {
      dayOffset += 24 * 3600;
    }

    let start = rawStart + dayOffset;
    let end = (rawEnd === null ? rawStart : rawEnd) + dayOffset;
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

async function buildLiveBusPaddleMappings() {
  const activePaddles = getActivePaddlesForNow();
  if (!activePaddles.length) return new Map();

  const mappings = new Map();
  const concurrency = Math.min(Math.max(2, TRACK_CONCURRENCY), 6);
  let index = 0;

  async function worker() {
    while (index < activePaddles.length) {
      const currentIndex = index;
      index += 1;
      const activePaddle = activePaddles[currentIndex];

      try {
        const payload = await fetchLiveResultWithFallback(activePaddle.block);
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
            verifiedAt: new Date().toISOString(),
          });
        }
      } catch (_) {
        // Ignore per-paddle failures so one bad lookup doesn't block the cache.
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return mappings;
}

function buildLiveBusPaddleRow(busNumber, value) {
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
    verified_at: String(value.verifiedAt || new Date().toISOString()),
  };
}

function rememberLiveBusPaddleMapping(busNumber, value) {
  const normalizedBus = String(busNumber || '').trim();
  if (!normalizedBus || !value?.block) return;
  const entry = {
    block: String(value.block || '').trim(),
    paddleId: String(value.paddleId || '').trim(),
    serviceDay: String(value.serviceDay || '').trim(),
    route: String(value.route || '').trim(),
    headsign: String(value.headsign || '').trim(),
    tripNumber: String(value.tripNumber || '').trim(),
    startTime: String(value.startTime || '').trim(),
    endTime: String(value.endTime || '').trim(),
    verifiedAt: String(value.verifiedAt || new Date().toISOString()),
  };
  liveBusPaddleCache.set(normalizedBus, entry);
  busBlockCache.set(normalizedBus, {
    block: entry.block,
    expiresAt: Date.now() + 3 * 60 * 1000,
  });
}

function isLiveBusMappingFresh(value) {
  if (!value?.block || !value?.verifiedAt) return false;
  const verifiedAtMs = Date.parse(String(value.verifiedAt));
  if (!Number.isFinite(verifiedAtMs)) return false;
  return Date.now() - verifiedAtMs <= LIVE_BUS_MAPPING_TTL_MS;
}

async function persistLiveBusPaddleMappings(mappings) {
  if (!adminSupabase) {
    return { persisted: false, reason: 'missing-service-role' };
  }

  const rows = Array.from(mappings.entries())
    .map(([busNumber, value]) => buildLiveBusPaddleRow(busNumber, value))
    .filter((row) => row.bus_number && row.block);

  if (!rows.length) {
    return { persisted: false, reason: 'no-live-mappings' };
  }

  const snapshotIso = new Date().toISOString();
  for (const row of rows) {
    row.verified_at = snapshotIso;
  }

  const { error: upsertError } = await adminSupabase
    .from('live_bus_paddles')
    .upsert(rows, { onConflict: 'bus_number' });
  if (upsertError) {
    throw upsertError;
  }

  const { error: deleteError } = await adminSupabase
    .from('live_bus_paddles')
    .delete()
    .lt('verified_at', snapshotIso);
  if (deleteError) {
    throw deleteError;
  }

  for (const row of rows) {
    rememberLiveBusPaddleMapping(row.bus_number, {
      block: row.block,
      paddleId: row.paddle_id,
      serviceDay: row.service_day,
      route: row.route,
      headsign: row.headsign,
      tripNumber: row.trip_number,
      startTime: row.start_time,
      endTime: row.end_time,
      verifiedAt: row.verified_at,
    });
  }

  return { persisted: true, count: rows.length, verifiedAt: snapshotIso };
}

async function refreshLiveBusPaddleMappings() {
  if (liveBusPaddleRefreshPromise) {
    return liveBusPaddleRefreshPromise;
  }

  liveBusPaddleRefreshPromise = (async () => {
    const mappings = await buildLiveBusPaddleMappings();
    return persistLiveBusPaddleMappings(mappings);
  })().finally(() => {
    liveBusPaddleRefreshPromise = null;
  });

  return liveBusPaddleRefreshPromise;
}

async function getStoredLiveBusPaddleMapping(busNumber) {
  const normalizedBus = String(busNumber || '').trim();
  if (!normalizedBus) return null;

  const cached = liveBusPaddleCache.get(normalizedBus);
  if (cached && isLiveBusMappingFresh(cached)) {
    return cached;
  }

  if (!adminSupabase) {
    return null;
  }

  const cutoffIso = new Date(Date.now() - LIVE_BUS_MAPPING_TTL_MS).toISOString();
  const { data, error } = await adminSupabase
    .from('live_bus_paddles')
    .select('bus_number, block, paddle_id, service_day, route, trip_number, headsign, start_time, end_time, verified_at')
    .eq('bus_number', normalizedBus)
    .gte('verified_at', cutoffIso)
    .maybeSingle();

  if (error) {
    throw error;
  }
  if (!data?.block) {
    return null;
  }

  const mapping = {
    block: data.block,
    paddleId: data.paddle_id,
    serviceDay: data.service_day,
    route: data.route,
    tripNumber: data.trip_number,
    headsign: data.headsign,
    startTime: data.start_time,
    endTime: data.end_time,
    verifiedAt: data.verified_at,
  };
  rememberLiveBusPaddleMapping(normalizedBus, mapping);
  return mapping;
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

function buildShuttleResponse(id, requestedDay) {
  const serviceDay = normalizeServiceDay(requestedDay) || getOttawaServiceDayKey();
  const availableIds = SHUTTLES_BY_SERVICE_DAY[serviceDay] || [];
  if (!availableIds.includes(id)) return null;

  const shuttle = getShuttleForToday(id);
  if (!shuttle) return null;
  const currentServiceDay = getOttawaServiceDayKey();
  const isLiveDay = serviceDay === currentServiceDay;
  const nextStop = isLiveDay ? describeNextShuttleStop(shuttle) : null;
  const suggestedLiveShuttle = !isLiveDay
    ? findEquivalentShuttleForDay(id, currentServiceDay)
    : null;

  return {
    ok: true,
    id: shuttle.id,
    route: shuttle.route,
    name: shuttle.name,
    serviceDay,
    isLiveDay,
    currentServiceDay,
    sourceLabel: shuttle.sourceLabel,
    sourceFile: shuttle.sourceFile,
    stops: shuttle.stops,
    trips: shuttle.trips,
    nextStop,
    suggestedLiveShuttle: suggestedLiveShuttle ? {
      id: suggestedLiveShuttle.id,
      route: suggestedLiveShuttle.route,
      name: suggestedLiveShuttle.name,
      serviceDay: currentServiceDay,
      label: `${suggestedLiveShuttle.route} ${suggestedLiveShuttle.name}`,
    } : null,
  };
}

async function fetchTripsForBlock(block) {
  return fetchTripsForResolvedBlock(block);
}

function loadPaddleIndex() {
  if (!paddleIndexCache) {
    const filePath = path.join(__dirname, 'data', 'paddles.index.json');
    paddleIndexCache = sanitizePaddleIndex(JSON.parse(fs.readFileSync(filePath, 'utf8')));
  }
  return paddleIndexCache;
}

function isUsablePaddleTrip(trip) {
  if (!trip || typeof trip !== 'object') return false;
  const tripNumber = Number(trip.trip_number ?? trip.tripNumber ?? 0);
  const startTime = String(trip.start_time || trip.startTime || '').trim();
  const endTime = String(trip.end_time || trip.endTime || '').trim();
  const startStop = String(trip.start_stop || trip.startStop || '').trim();
  const endStop = String(trip.end_stop || trip.endStop || '').trim();
  const headsign = String(trip.headsign || trip.headSign || '').trim();

  if (tripNumber <= 0 && !startTime && !endTime) return false;
  if (!startTime && !endTime && !startStop && !endStop) return false;
  if (!headsign && !startTime && !endTime) return false;
  return true;
}

function sanitizeRunTrips(run) {
  if (!run || typeof run !== 'object') return run;
  const trips = Array.isArray(run.trips) ? run.trips.filter(isUsablePaddleTrip) : [];
  return {
    ...run,
    trips,
  };
}

function sanitizePaddleIndex(index) {
  if (!index || typeof index !== 'object') return index;
  const serviceDays = {};
  for (const [serviceDay, runs] of Object.entries(index.service_days || {})) {
    const sanitizedRuns = {};
    for (const [paddleId, run] of Object.entries(runs || {})) {
      sanitizedRuns[paddleId] = sanitizeRunTrips(run);
    }
    serviceDays[serviceDay] = sanitizedRuns;
  }

  const variants = {};
  for (const [variantId, variant] of Object.entries(index.variants || {})) {
    const sanitizedVariantServiceDays = {};
    for (const [serviceDay, runs] of Object.entries(variant.service_days || {})) {
      const sanitizedRuns = {};
      for (const [paddleId, run] of Object.entries(runs || {})) {
        sanitizedRuns[paddleId] = sanitizeRunTrips(run);
      }
      sanitizedVariantServiceDays[serviceDay] = sanitizedRuns;
    }
    variants[variantId] = {
      ...variant,
      service_days: sanitizedVariantServiceDays,
    };
  }

  return {
    ...index,
    service_days: serviceDays,
    variants,
  };
}

function getPaddleVariants() {
  const index = loadPaddleIndex();
  if (index?.variants && typeof index.variants === 'object') {
    return index.variants;
  }
  return {
    current: {
      label: 'Current spring paddles',
      activation_date: null,
      service_days: index?.service_days || {},
    },
  };
}

function getPaddleVariantMeta(variantId) {
  const variants = getPaddleVariants();
  return variants?.[variantId] || null;
}

function blockToPaddleId(block) {
  const match = String(block || '').trim().toUpperCase().match(/^([A-Z0-9]+)-(\d{1,3})$/);
  if (!match) return null;
  return `${match[1].padStart(3, '0')}${match[2].padStart(3, '0')}`;
}

function isApril19PaddleVariantActive(referenceDate = new Date()) {
  return getOttawaDateString(referenceDate) >= APRIL19_PADDLE_SWITCH_DATE;
}

function getDefaultPaddleVariantIdForDate(referenceDate = new Date(), serviceDay = '') {
  if (serviceDay === 'easter_monday') return 'current';
  return isApril19PaddleVariantActive(referenceDate) ? 'april19' : 'current';
}

function getPaddleDisplayVariantLabel(referenceDate = new Date(), variantId = '', serviceDay = '') {
  if (serviceDay === 'easter_monday') return null;
  if (!isApril19PaddleVariantActive(referenceDate) && variantId === 'april19') {
    return `${formatServiceDayLabel(serviceDay)} paddles (Spring)`;
  }
  return null;
}

function getPaddleRunForVariant(variantId, serviceDay, paddleId) {
  if (!variantId || !serviceDay || !paddleId) return null;
  const variant = getPaddleVariantMeta(variantId);
  return variant?.service_days?.[serviceDay]?.[paddleId] || null;
}

async function fetchPaddleTripsForBlock(block) {
  const paddleId = blockToPaddleId(block);
  if (!paddleId) return [];

  const resolved = resolvePaddleRunForCurrentContext(paddleId);
  const run = resolved?.run;
  if (!run || !Array.isArray(run.trips)) {
    return [];
  }

  const previousServiceDay = getServiceDayKeyForDate(getPreviousOttawaDate(new Date()));
  const serviceDate = resolved?.carryover && resolved?.serviceDay === previousServiceDay
    ? formatOttawaDateForTransSee(getPreviousOttawaDate(new Date()))
    : getOttawaServiceDateString();

  return run.trips.map((trip) => ({
    tripNumber: Number(trip.trip_number || 0) || null,
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
    serviceDate,
  })).filter((trip) => trip.routeId && trip.scheduledStartTime);
}

function getPaddleRunForDay(serviceDay, paddleId, options = {}) {
  const referenceDate = options.referenceDate || new Date();
  const preferredVariantId = options.variantId || getDefaultPaddleVariantIdForDate(referenceDate, serviceDay);
  const preferredRun = getPaddleRunForVariant(preferredVariantId, serviceDay, paddleId);
  if (preferredRun) return preferredRun;

  if (preferredVariantId !== 'current') {
    const fallbackCurrent = getPaddleRunForVariant('current', serviceDay, paddleId);
    if (fallbackCurrent) return fallbackCurrent;
  }
  if (preferredVariantId !== 'april19') {
    const fallbackApril19 = getPaddleRunForVariant('april19', serviceDay, paddleId);
    if (fallbackApril19) return fallbackApril19;
  }

  const index = loadPaddleIndex();
  return index?.service_days?.[serviceDay]?.[paddleId] || null;
}

function getPaddleOptionsForBlock(block) {
  const paddleId = blockToPaddleId(block);
  if (!paddleId) return [];

  const referenceDate = new Date();
  const beforeSwitch = !isApril19PaddleVariantActive(referenceDate);
  const options = [];

  const addOption = (serviceDay, run, variantId) => {
    if (!run) return;
    const variantLabel = run.variant_label || getPaddleVariantMeta(variantId)?.label || null;
    const displayVariantLabel = getPaddleDisplayVariantLabel(referenceDate, variantId, serviceDay);
    const currentDefaultVariantId = getDefaultPaddleVariantIdForDate(referenceDate, serviceDay);
    let buttonLabel = `${formatServiceDayLabel(serviceDay)}`;
    if (beforeSwitch && serviceDay !== 'easter_monday') {
      buttonLabel = variantId === 'april19'
        ? `${formatServiceDayLabel(serviceDay)} (Spring)`
        : `${formatServiceDayLabel(serviceDay)}`;
    } else if (variantId !== currentDefaultVariantId && serviceDay !== 'easter_monday') {
      buttonLabel = `${formatServiceDayLabel(serviceDay)} (${variantLabel || variantId})`;
    }
    options.push({
      serviceDay,
      sourceId: run.source_id || null,
      sourceLabel: run.source_label || null,
      effective: run.effective || null,
      variantId: variantId || run.variant_id || null,
      variantLabel,
      displayVariantLabel,
      buttonLabel,
    });
  };

  for (const serviceDay of ['weekday', 'saturday', 'sunday']) {
    if (beforeSwitch) {
      addOption(serviceDay, getPaddleRunForVariant('current', serviceDay, paddleId), 'current');
      addOption(serviceDay, getPaddleRunForVariant('april19', serviceDay, paddleId), 'april19');
    } else {
      const preferredRun = getPaddleRunForDay(serviceDay, paddleId, {
        referenceDate,
        variantId: getDefaultPaddleVariantIdForDate(referenceDate, serviceDay),
      });
      addOption(serviceDay, preferredRun, preferredRun?.variant_id || getDefaultPaddleVariantIdForDate(referenceDate, serviceDay));
    }
  }

  if (isEasterMondayOptionVisible(referenceDate)) {
    addOption('easter_monday', getPaddleRunForDay('easter_monday', paddleId, {
      referenceDate,
      variantId: 'current',
    }), 'current');
  }

  const currentServiceDay = getOttawaServiceDayKey();
  const baseOrder = new Map([
    ['weekday', 0],
    ['saturday', 1],
    ['sunday', 2],
    ['easter_monday', 3],
  ]);
  const seen = new Set();
  return options.filter((option) => {
    const key = `${option.serviceDay}|${option.variantId || ''}|${option.sourceId || ''}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).sort((a, b) => {
    if (a.serviceDay === currentServiceDay && b.serviceDay !== currentServiceDay) return -1;
    if (b.serviceDay === currentServiceDay && a.serviceDay !== currentServiceDay) return 1;
    if (a.serviceDay === b.serviceDay) {
      const defaultVariantId = getDefaultPaddleVariantIdForDate(referenceDate, a.serviceDay);
      if (a.variantId === defaultVariantId && b.variantId !== defaultVariantId) return -1;
      if (b.variantId === defaultVariantId && a.variantId !== defaultVariantId) return 1;
    }
    return (baseOrder.get(a.serviceDay) ?? 99) - (baseOrder.get(b.serviceDay) ?? 99);
  });
}

function getPreviousOttawaDate(date = new Date()) {
  return new Date(date.getTime() - 24 * 3600 * 1000);
}

function formatOttawaDateForTransSee(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function resolvePaddleRunForCurrentContext(paddleId) {
  if (!paddleId) return null;

  const now = new Date();
  const previousDate = getPreviousOttawaDate(now);
  const currentServiceDay = getServiceDayKeyForDate(now);
  const previousServiceDay = getServiceDayKeyForDate(previousDate);
  const nowSeconds = getOttawaNowSeconds();
  const currentVariantId = getDefaultPaddleVariantIdForDate(now, currentServiceDay);
  const previousVariantId = getDefaultPaddleVariantIdForDate(previousDate, previousServiceDay);

  const currentRun = getPaddleRunForDay(currentServiceDay, paddleId, { referenceDate: now, variantId: currentVariantId });
  const previousRun = getPaddleRunForDay(previousServiceDay, paddleId, { referenceDate: previousDate, variantId: previousVariantId });

  const previousActiveTrip = previousRun
    ? findActiveTripInRun(previousRun, nowSeconds + 24 * 3600)
    : null;
  if (previousActiveTrip && previousActiveTrip.end > 24 * 3600) {
    return {
      serviceDay: previousServiceDay,
      variantId: previousRun?.variant_id || previousVariantId,
      variantLabel: previousRun?.variant_label || getPaddleVariantMeta(previousVariantId)?.label || null,
      run: previousRun,
      activeTrip: previousActiveTrip,
      carryover: true,
    };
  }

  if (currentRun) {
    return {
      serviceDay: currentServiceDay,
      variantId: currentRun?.variant_id || currentVariantId,
      variantLabel: currentRun?.variant_label || getPaddleVariantMeta(currentVariantId)?.label || null,
      run: currentRun,
      activeTrip: currentRun ? findActiveTripInRun(currentRun, nowSeconds) : null,
      carryover: false,
    };
  }

  if (previousRun) {
    return {
      serviceDay: previousServiceDay,
      variantId: previousRun?.variant_id || previousVariantId,
      variantLabel: previousRun?.variant_label || getPaddleVariantMeta(previousVariantId)?.label || null,
      run: previousRun,
      activeTrip: previousActiveTrip,
      carryover: Boolean(previousActiveTrip),
    };
  }

  return null;
}

function buildPaddleResponse(block, requestedDay = '', requestedVariant = '') {
  const paddleId = blockToPaddleId(block);
  if (!paddleId) return null;

  const now = new Date();
  const explicitDay = normalizeServiceDay(requestedDay);
  const explicitVariantId = String(requestedVariant || '').trim().toLowerCase();
  const currentResolved = resolvePaddleRunForCurrentContext(paddleId);
  const resolvedExplicitVariantId = explicitDay
    ? (explicitVariantId || getDefaultPaddleVariantIdForDate(now, explicitDay))
    : '';
  const explicitRun = explicitDay
    ? getPaddleRunForDay(explicitDay, paddleId, { referenceDate: now, variantId: resolvedExplicitVariantId })
    : null;
  const resolved = explicitDay
    ? {
        serviceDay: explicitDay,
        variantId: explicitRun?.variant_id || resolvedExplicitVariantId || null,
        variantLabel: explicitRun?.variant_label || getPaddleVariantMeta(resolvedExplicitVariantId)?.label || null,
        run: explicitRun,
        activeTrip:
          currentResolved?.serviceDay === explicitDay &&
          currentResolved?.variantId === (explicitRun?.variant_id || resolvedExplicitVariantId || null)
            ? (currentResolved.activeTrip || null)
            : null,
        carryover: false,
      }
    : resolvePaddleRunForCurrentContext(paddleId);
  if (!resolved || !resolved.run) return null;
  const { serviceDay, variantId, variantLabel, run, carryover } = resolved;

  return {
    block: String(block),
    paddleId,
    serviceDay,
    variantId: variantId || run.variant_id || null,
    variantLabel: variantLabel || run.variant_label || null,
    displayVariantLabel: getPaddleDisplayVariantLabel(now, variantId || run.variant_id || null, serviceDay),
    carryover,
    sourceId: run.source_id || null,
    sourceLabel: run.source_label || null,
    effective: run.effective || null,
    garage: run.garage || null,
    signOn: run.sign_on || null,
    routes: Array.isArray(run.routes) ? run.routes : [],
    busType: run.bus_type || null,
    activeTrip: resolved.activeTrip || null,
    currentServiceDay: currentResolved?.serviceDay || null,
    isLiveDay: Boolean(resolved.activeTrip),
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

  const scheduledBase = [...trips]
    .filter((trip) => trip && trip.sourceType === 'paddle')
    .map((trip) => ({
      trip,
      start: timeToSeconds(trip.scheduledStartTime),
      end: timeToSeconds(trip.scheduledEndTime),
    }))
    .filter((entry) => entry.start !== null);

  if (!scheduledBase.length) return [];

  let dayOffset = 0;
  let previousStart = null;
  const scheduled = scheduledBase
    .sort((a, b) => {
      const aTrip = Number(a.trip?.tripNumber) || 0;
      const bTrip = Number(b.trip?.tripNumber) || 0;
      if (aTrip && bTrip) return aTrip - bTrip;
      return a.start - b.start;
    })
    .map((entry) => {
      if (previousStart !== null && entry.start < previousStart) {
        dayOffset += 24 * 3600;
      }
      previousStart = entry.start;
      const start = entry.start + dayOffset;
      let end = (entry.end ?? entry.start) + dayOffset;
      if (end < start) end += 24 * 3600;
      return { ...entry, start, end };
    })
    .map((entry, index, arr) => ({ ...entry, index, total: arr.length }));

  const compareNow = nowSeconds < 4 * 3600 && scheduled.some((entry) => entry.end > 24 * 3600)
    ? nowSeconds + 24 * 3600
    : nowSeconds;

  const currentIndex = scheduled.findIndex((entry) =>
    entry.end !== null
      ? entry.start <= compareNow && compareNow <= entry.end + 20 * 60
      : Math.abs(compareNow - entry.start) <= 20 * 60
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
      if (previousEnd !== null && compareNow - previousEnd <= 45 * 60) {
        push(previous);
      }
    }

    const next = scheduled[currentIndex + 1];
    if (next && next.start - compareNow <= 20 * 60) {
      push(next);
    }

    push(scheduled[currentIndex - 2]);
    push(scheduled[currentIndex + 2]);
    return candidates;
  }

  const nextIndex = scheduled.findIndex((entry) => entry.start > compareNow);
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

async function getActivePaddlesWithTrips(preferredBlock = '') {
  const activePaddles = getActivePaddlesForNow();
  const ordered = [...activePaddles].sort((a, b) => {
    if (preferredBlock && a.block === preferredBlock) return -1;
    if (preferredBlock && b.block === preferredBlock) return 1;
    return 0;
  });
  return Promise.all(ordered.map(async (item) => ({
    ...item,
    trips: await fetchPaddleTripsForBlock(item.block),
  })));
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

async function resolveBlockForBus(busNumber) {
  const normalizedBus = String(busNumber || '').trim();
  const cached = busBlockCache.get(normalizedBus);
  const now = Date.now();
  if (cached && cached.expiresAt > now) {
    return cached.block;
  }

  const confirmed = await getStoredLiveBusPaddleMapping(normalizedBus).catch(() => null);
  if (confirmed?.block) {
    busBlockCache.set(normalizedBus, {
      block: confirmed.block,
      expiresAt: now + 3 * 60 * 1000,
    });
    return confirmed.block;
  }
  return null;
}

function extractTransSeeCoordinates(html, busNumber) {
  const escapedBus = String(busNumber || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const specificPattern = new RegExp(
    `AddMarker\\(\\[\\s*(-?\\d+(?:\\.\\d+)?)\\s*,\\s*(-?\\d+(?:\\.\\d+)?)\\s*\\][\\s\\S]*?"${escapedBus}"\\s*,"Coctranspo`,
    'i'
  );
  const specificMatch = String(html || '').match(specificPattern);
  if (specificMatch) {
    const latitude = Number(specificMatch[1]);
    const longitude = Number(specificMatch[2]);
    if (Number.isFinite(latitude) && Number.isFinite(longitude)) {
      return {
        latitude: Number(latitude.toFixed(6)),
        longitude: Number(longitude.toFixed(6)),
      };
    }
  }

  const genericMatch = String(html || '').match(/AddMarker\(\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]/i);
  if (genericMatch) {
    const latitude = Number(genericMatch[1]);
    const longitude = Number(genericMatch[2]);
    if (Number.isFinite(latitude) && Number.isFinite(longitude)) {
      return {
        latitude: Number(latitude.toFixed(6)),
        longitude: Number(longitude.toFixed(6)),
      };
    }
  }

  return null;
}

async function fetchLocationForBus(busNumber) {
  const url = `https://transsee.ca/fleetfind?a=octranspo&q=${encodeURIComponent(busNumber)}&Go=Go`;
  const html = await fetchTransSeeText(url);
  const lines = htmlToLines(html);
  const locationText = pickBestLocationLine(lines, busNumber);
  const coords = extractTransSeeCoordinates(html, busNumber);

  if (!locationText) {
    throw Object.assign(new Error(`No location found for bus ${busNumber}`), { code: 404 });
  }

  return {
    busNumber: String(busNumber),
    locationText,
    url,
    latitude: coords?.latitude ?? null,
    longitude: coords?.longitude ?? null,
  };
}

function buildGtfsLocationForBus(busNumber, position = null, match = null) {
  const latitude = Number(position?.latitude);
  const longitude = Number(position?.longitude);
  const hasCoords = Number.isFinite(latitude) && Number.isFinite(longitude);
  const routeId = String(match?.paddleTrip?.routeId || position?.routeShortName || position?.routeId || '').trim();
  const headSign = String(match?.paddleTrip?.headSign || position?.headsign || '').trim();
  const stopName = String(position?.stopName || '').trim();
  const directionText = routeId && headSign
    ? `on route ${routeId} toward ${headSign}`
    : routeId
      ? `on route ${routeId}`
      : headSign
        ? `toward ${headSign}`
        : 'location available in GTFS-RT';
  const locationText = stopName ? `at ${stopName}` : `GTFS-RT ${directionText}`.trim();
  return {
    busNumber: String(busNumber),
    locationText,
    latitude: hasCoords ? Number(latitude.toFixed(6)) : null,
    longitude: hasCoords ? Number(longitude.toFixed(6)) : null,
  };
}

function buildCurrentTripSummary(trip = null) {
  if (!trip) return null;

  const route = String(trip.route || trip.routeId || '').trim();
  const headsign = String(trip.headsign || trip.headSign || '').trim();
  const tripNumber = String(trip.tripNumber || trip.trip_number || '').trim();
  const startTime = String(trip.startTime || trip.start_time || '').trim();
  const endTime = String(trip.endTime || trip.end_time || '').trim();
  const label = [route, headsign].filter(Boolean).join(' ').trim();

  if (!route && !headsign && !tripNumber && !startTime && !endTime) {
    return null;
  }

  return {
    route,
    headsign,
    tripNumber,
    startTime,
    endTime,
    label,
  };
}

function buildCurrentTripSummaryFromGtfsPosition(position = null) {
  if (!position) return null;
  return buildCurrentTripSummary({
    route: position.routeShortName || position.routeId || '',
    headsign: position.headsign || '',
  });
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

  const serviceDate = String(trip.serviceDate || getOttawaServiceDateString()).trim();
  const url = `https://www.transsee.ca/routesched?a=octranspo&r=${encodeURIComponent(route)}&date=${encodeURIComponent(serviceDate)}`;
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

function extractBusNumberFromTripSched(html, tripId) {
  const blockSectionMatch = html.match(/<div id=block><h4>Trips in this block<\/h4>([\s\S]*?)<\/table><\/div>/i);
  if (!blockSectionMatch) return null;

  const section = blockSectionMatch[1];
  const escapedTripId = String(tripId || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const rowRegex = /<tr[\s\S]*?<\/tr>/gi;
  let tripPathCell = '';
  let rowMatch;
  while ((rowMatch = rowRegex.exec(section))) {
    const row = rowMatch[0];
    if (escapedTripId && !new RegExp(`tripsched\\?a=octranspo&t=${escapedTripId}(?:&|")`, 'i').test(row)) {
      continue;
    }
    const cellMatches = [...row.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((match) => match[1]);
    tripPathCell = cellMatches[cellMatches.length - 1] || '';
    break;
  }
  if (!tripPathCell) return null;
  const busMatch = tripPathCell.match(/>(\d{3,5})<\/a>/);
  return busMatch ? busMatch[1] : null;
}

async function fetchBusFromTransSeeTrip(trip) {
  const date = String(trip.serviceDate || getOttawaServiceDateString()).trim();
  const tripId = trip.tripId || await fetchTripIdFromTransSeeRouteSchedule(trip);
  if (!tripId) return null;

  const url = `https://www.transsee.ca/tripsched?a=octranspo&t=${encodeURIComponent(tripId)}&date=${encodeURIComponent(date)}`;
  const html = await fetchTransSeeText(url, FALLBACK_TIMEOUT_MS);
  const busNumber = extractBusNumberFromTripSched(html, tripId);
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

async function fetchGtfsBlockFallback(block, trips) {
  if (!isGtfsRtConfigured() || !Array.isArray(trips) || !trips.length) {
    return null;
  }

  let payload;
  try {
    payload = await lookupBlockWithGtfsRt(block, trips);
  } catch (_) {
    return null;
  }

  const seen = new Set();
  const vehicleMatches = [];
  for (const match of payload?.matches || []) {
    const vehicleId = String(match?.vehicleId || '').trim();
    if (!vehicleId || seen.has(vehicleId)) continue;
    seen.add(vehicleId);
    vehicleMatches.push(match);
  }
  if (!vehicleMatches.length) {
    return null;
  }

  const primary = vehicleMatches[0];
  return {
    block,
    buses: [buildGtfsLocationForBus(primary.vehicleId, primary.position, primary)],
    liveSource: 'gtfs-rt',
    gtfsMatch: primary,
  };
}

async function fetchLiveResult(block) {
  if (pendingByBlock.has(block)) {
    return pendingByBlock.get(block);
  }

  const job = enqueue(async () => {
    const timings = {
      tripsFetchMs: 0,
      paddleFetchMs: 0,
      gtfsMs: 0,
      betterTransitMs: 0,
      transSeeMs: 0,
    };
    let trips = [];
    let paddleTrips = [];
    let directLookupError = null;

    try {
      const startedAt = Date.now();
      trips = await fetchTripsForBlock(block);
      timings.tripsFetchMs = Date.now() - startedAt;
    } catch (err) {
      directLookupError = err;
    }

    try {
      const startedAt = Date.now();
      paddleTrips = await fetchPaddleTripsForBlock(block);
      timings.paddleFetchMs = Date.now() - startedAt;
    } catch (_) {
      paddleTrips = [];
    }

    const gtfsTrips = paddleTrips.length ? paddleTrips : trips;
    if (gtfsTrips.length > 0) {
      const startedAt = Date.now();
      const gtfsFallback = await fetchGtfsBlockFallback(block, gtfsTrips);
      timings.gtfsMs = Date.now() - startedAt;
      if (gtfsFallback) {
        return { ...gtfsFallback, timings };
      }
    }

    let buses = [];
    if (!directLookupError) {
      try {
        const startedAt = Date.now();
        buses = await fetchBusesForBlock(block);
        timings.betterTransitMs += Date.now() - startedAt;
      } catch (err) {
        directLookupError = err;
      }
    }
    if (buses.length > 0) {
      const startedAt = Date.now();
      const results = await Promise.allSettled(buses.map((bus) => fetchLocationForBus(bus)));
      timings.betterTransitMs += Date.now() - startedAt;
      const locations = results
        .filter((result) => result.status === 'fulfilled')
        .map((result) => result.value);
      if (locations.length > 0) {
        return { block, buses: locations, liveSource: 'bettertransit', timings };
      }
    }

    const fallbackTrips = trips.length ? trips : paddleTrips;
    if (fallbackTrips.length > 0) {
      const startedAt = Date.now();
      const transSeeFallback = await fetchTransSeeTripFallback(block, fallbackTrips);
      timings.transSeeMs = Date.now() - startedAt;
      if (transSeeFallback) {
        return { ...transSeeFallback, liveSource: 'transsee', timings };
      }
    } else if (directLookupError) {
      throw directLookupError;
    }

    return { block, buses: [], liveSource: 'none', timings };
  }).finally(() => {
    pendingByBlock.delete(block);
  });

  pendingByBlock.set(block, job);
  return job;
}

async function fetchLiveResultWithFallback(block) {
  const startedAt = Date.now();
  try {
    const payload = await withTimeout(fetchLiveResult(block), RUN_TIMEOUT_MS);
    return {
      ...payload,
      timings: {
        ...(payload?.timings || {}),
        totalLookupMs: Date.now() - startedAt,
      },
    };
  } catch (directErr) {
    if (!ENABLE_PLAYWRIGHT_FALLBACK) throw directErr;
    if (Number(directErr.code) === 400) throw directErr;
    const fallbackStartedAt = Date.now();
    const fallback = await withTimeout(trackBlock(block, { headless: true }), FALLBACK_TIMEOUT_MS);
    return {
      ...fallback,
      liveSource: fallback.liveSource || 'playwright-fallback',
      timings: {
        ...(fallback?.timings || {}),
        fallbackMs: Date.now() - fallbackStartedAt,
        totalLookupMs: Date.now() - startedAt,
      },
    };
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

function parseLookupTarget(req) {
  if (typeof req.query.block === 'string') {
    return { type: 'block', value: normalizeBlock(req.query.block) };
  }
  if (typeof req.query.bus === 'string') {
    return { type: 'bus', value: normalizeMessage(req.query.bus) };
  }

  const text = parseMessageText(req);
  const blockMatch = text.match(/\b(\d{1,3}-\d{1,3})\b/);
  if (blockMatch) {
    return { type: 'block', value: normalizeBlock(blockMatch[1]) };
  }
  const busMatch = text.match(/\b(\d{3,5})\b/);
  if (busMatch) {
    return { type: 'bus', value: normalizeMessage(busMatch[1]) };
  }
  return { type: 'block', value: normalizeBlock(text) };
}

function parseTimeParamToSeconds(value) {
  const text = String(value || '').trim();
  const match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;
  return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3] || 0);
}

function isLocalRequest(req) {
  const ip = String(req.ip || req.connection?.remoteAddress || '').trim();
  return ip === '127.0.0.1' || ip === '::1' || ip === '::ffff:127.0.0.1';
}

function isAuthorizedCronRequest(req) {
  const authHeader = String(req.get('authorization') || '').trim();
  if (CRON_SECRET && authHeader === `Bearer ${CRON_SECRET}`) {
    return true;
  }
  if (CRON_SECRET && String(req.query?.key || '').trim() === CRON_SECRET) {
    return true;
  }
  if (String(req.get('x-vercel-cron') || '').trim() === '1') {
    return true;
  }
  return isLocalRequest(req);
}

function isTrackRefreshRequest(req, target) {
  const refreshValue = normalizeMessage(req.query?.refresh || req.query?.job || '');
  if (!refreshValue) {
    return false;
  }
  if (target?.value) {
    return false;
  }
  if (String(req.get('x-vercel-cron') || '').trim() === '1') {
    return true;
  }
  return refreshValue === 'live-bus-paddles';
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

function describeLiveSource(value) {
  const source = String(value || '').trim().toLowerCase();
  if (source === 'gtfs-rt') return 'GTFS-RT';
  if (source === 'bettertransit') return 'BetterTransit';
  if (source === 'transsee') return 'TransSee';
  if (source === 'playwright-fallback') return 'Playwright fallback';
  if (source === 'none') return 'No live source';
  if (source === 'direct') return 'Direct lookup';
  return source ? source : 'Unknown';
}

function formatTimingLine(timings = {}) {
  const parts = [];
  const totalLookupMs = Number(timings?.totalLookupMs || 0);
  const gtfsMs = Number(timings?.gtfsMs || 0);
  const betterTransitMs = Number(timings?.betterTransitMs || 0);
  const transSeeMs = Number(timings?.transSeeMs || 0);
  const fallbackMs = Number(timings?.fallbackMs || 0);
  if (totalLookupMs > 0) parts.push(`total ${totalLookupMs}ms`);
  if (gtfsMs > 0) parts.push(`GTFS ${gtfsMs}ms`);
  if (betterTransitMs > 0) parts.push(`BetterTransit ${betterTransitMs}ms`);
  if (transSeeMs > 0) parts.push(`TransSee ${transSeeMs}ms`);
  if (fallbackMs > 0) parts.push(`fallback ${fallbackMs}ms`);
  return parts.length ? `Timing: ${parts.join(' | ')}` : '';
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
  if (payload?.currentTrip?.label) {
    const tripSuffix = payload.currentTrip.tripNumber ? ` (Trip ${payload.currentTrip.tripNumber})` : '';
    lines.push(`Current trip: ${payload.currentTrip.label}${tripSuffix}`);
  }
  return lines.join('\n');
}

function formatShuttleListReply(serviceDay, shuttles) {
  const label = serviceDay.replace(/_/g, ' ');
  if (!shuttles.length) {
    return `No shuttles are listed for ${label}.`;
  }
  const lines = [`Available shuttles for ${label}:`];
  for (const shuttle of shuttles) {
    lines.push(`${shuttle.route}: ${shuttle.name}`);
  }
  lines.push('Tap a shuttle below to view the schedule and next stop.');
  return lines.join('\n');
}

function formatShowAllReply(activePaddles) {
  if (!activePaddles.length) {
    return 'No paddles look active right now.';
  }

  const lines = [`Active paddles right now (${activePaddles.length}):`];
  for (const item of activePaddles) {
    const routePart = item.route ? `Route ${item.route}` : 'Route n/a';
    const headsignPart = item.headsign ? ` | ${item.headsign}` : '';
    lines.push(`${item.block} | Trip ${item.tripNumber} | ${routePart}${headsignPart} | ${item.startTime}-${item.endTime}`);
  }
  return lines.join('\n');
}

function formatBusReply(payload) {
  const buses = Array.isArray(payload?.buses) ? payload.buses : [];
  if (!buses.length) {
    return `Bus ${payload?.busNumber || ''}: no live location is available right now.`.trim();
  }

  const lines = [`Bus ${payload.busNumber}`];
  for (const bus of buses) {
    lines.push(`${bus.locationText}`);
  }
  if (payload?.currentTrip?.label) {
    const tripSuffix = payload.currentTrip.tripNumber ? ` (Trip ${payload.currentTrip.tripNumber})` : '';
    lines.push(`Current trip: ${payload.currentTrip.label}${tripSuffix}`);
  }
  if (payload.block) {
    lines.push(`Current block: ${payload.block}`);
  } else if (payload.parked) {
    lines.push('Status: parked');
  }
  return lines.join('\n');
}

function locationSuggestsParked(locationText) {
  const value = String(locationText || '').toLowerCase();
  if (!value) return false;
  return /\b(garage|depot|parked)\b/i.test(value);
}

async function handleBusLookup(busNumber, res) {
  if (!isLikelyBusNumber(busNumber)) {
    res.status(400).json({ ok: false, error: 'Bus format must look like 6448.' });
    return;
  }

  try {
    const startedAt = Date.now();
    let payload = null;
    const storedMapping = await getStoredLiveBusPaddleMapping(busNumber).catch(() => null);
    let gtfsMatched = null;

    if (isGtfsRtConfigured()) {
      const gtfsStartedAt = Date.now();
      try {
        let cachedBlock = storedMapping?.block || await resolveBlockForBus(busNumber).catch(() => null);
        const gtfsPayload = await lookupBusPositionWithGtfsRt(busNumber);
        if (gtfsPayload?.position) {
          const routeHint = String(gtfsPayload.position.routeShortName || gtfsPayload.position.routeId || '').trim();
          const activeCandidates = getActivePaddlesForNow();
          const filteredCandidates = routeHint
            ? activeCandidates.filter((item) => String(item.route || '').trim() === routeHint)
            : activeCandidates;
          const lookupCandidates = filteredCandidates.length ? filteredCandidates : activeCandidates;
          if (lookupCandidates.length) {
            const activePaddlesWithTrips = await Promise.all(
              lookupCandidates.map(async (item) => ({
                ...item,
                trips: await fetchPaddleTripsForBlock(item.block),
              }))
            );
            gtfsMatched = await withTimeout(
              lookupBusWithGtfsRt(busNumber, activePaddlesWithTrips),
              2500
            ).catch(() => null);
          }

          if (!cachedBlock && gtfsMatched?.matched?.block) {
            cachedBlock = gtfsMatched.matched.block;
          }
          if (!cachedBlock && gtfsPayload.position.blockId) {
            cachedBlock = await resolveCanonicalBlock(gtfsPayload.position.blockId).catch(() => null) || normalizeBlock(gtfsPayload.position.blockId);
          }
          const gtfsBus = buildGtfsLocationForBus(busNumber, gtfsPayload.position, gtfsMatched?.matched || null);
          payload = {
            busNumber: String(busNumber),
            block: cachedBlock || null,
            buses: [gtfsBus],
            gtfsPosition: gtfsPayload.position,
            gtfsMatched: gtfsMatched?.matched || null,
            parked: !cachedBlock && locationSuggestsParked(gtfsBus?.locationText),
            liveSource: 'gtfs-rt',
            timings: {
              gtfsMs: Date.now() - gtfsStartedAt,
            },
          };
        }
      } catch (_) {
        // Fall through to the older location path below.
      }
    }

    if (!payload) {
      const [location, block] = await Promise.all([
        fetchLocationForBus(busNumber),
        withTimeout(resolveBlockForBus(busNumber).catch(() => null), 10000).catch(() => null),
      ]);
      payload = {
        busNumber: String(busNumber),
        block: block || null,
        buses: [location],
        parked: !block && locationSuggestsParked(location?.locationText),
        liveSource: 'transsee',
        timings: {},
      };
    }

    payload.timings = {
      ...(payload.timings || {}),
      totalLookupMs: Date.now() - startedAt,
    };
    const paddle = payload.block ? buildPaddleResponse(payload.block) : null;
    const currentTrip = buildCurrentTripSummary(
      paddle?.activeTrip ||
      payload?.gtfsMatched?.paddleTrip ||
      (payload.liveSource === 'gtfs-rt' ? buildCurrentTripSummaryFromGtfsPosition(payload.gtfsPosition || null) : null) ||
      storedMapping
    );
    const paddleOptions = payload.block ? getPaddleOptionsForBlock(payload.block) : [];

    res.json({
      ok: true,
      mode: 'bus',
      busNumber: payload.busNumber,
      block: payload.block,
      buses: payload.buses,
      currentTrip,
      paddleAvailable: Boolean(paddle),
      paddleOptions,
      cached: false,
      responseMs: payload.timings.totalLookupMs,
      timings: payload.timings,
      reply: formatBusReply({ ...payload, currentTrip }),
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

async function handleLookup(req, res) {
  const target = parseLookupTarget(req);
  if (isTrackRefreshRequest(req, target)) {
    return handleRefreshLiveBusPaddles(req, res);
  }
  if (target.type === 'bus') {
    return handleBusLookup(target.value, res);
  }

  const rawBlock = target.value;
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const startedAt = Date.now();
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
    const responseMs = Date.now() - startedAt;
    const paddle = buildPaddleResponse(block);
    const currentTrip = buildCurrentTripSummary(paddle?.activeTrip || payload?.gtfsMatch?.paddleTrip);
    const paddleOptions = getPaddleOptionsForBlock(block);
    res.json({
      ok: true,
      block: payload.block,
      buses: payload.buses,
      liveSource: payload.liveSource || 'direct',
      currentTrip,
      paddleAvailable: Boolean(paddle),
      paddleOptions,
      cached: false,
      responseMs,
      timings: {
        ...(payload?.timings || {}),
        totalRequestMs: responseMs,
      },
      reply: formatChatReply({ ...payload, currentTrip }),
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
  if (isShowAllRequest(message)) {
    const activePaddles = getActivePaddlesForNow();
    res.json({
      ok: true,
      mode: 'showall',
      reply: formatShowAllReply(activePaddles),
      activePaddles,
      generatedAt: new Date().toISOString(),
    });
    return;
  }

  if (isShuttleRequest(message)) {
    const serviceDay = parseRequestedShuttleDay(message) || getOttawaServiceDayKey();
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
      shuttleDay: serviceDay,
      shuttleDayOptions: SHUTTLE_DAY_OPTIONS,
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
    const requestedDay = normalizeServiceDay(req.query.day);
    const requestedVariant = String(req.query.variant || '').trim().toLowerCase();
    const paddle = buildPaddleResponse(block, requestedDay, requestedVariant);
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
  const requestedDay = normalizeServiceDay(req.query.day);
  if (!shuttleId) {
    res.status(400).json({
      ok: false,
      error: 'Choose a shuttle first.',
    });
    return;
  }

  const shuttle = buildShuttleResponse(shuttleId, requestedDay);
  if (!shuttle) {
    res.status(404).json({
      ok: false,
      error: `Shuttle not found: ${shuttleId}`,
    });
    return;
  }

  res.json(shuttle);
}

async function handleRefreshLiveBusPaddles(req, res) {
  if (!isAuthorizedCronRequest(req)) {
    res.status(401).json({
      ok: false,
      error: 'Unauthorized cron request.',
    });
    return;
  }

  if (!adminSupabase) {
    res.status(501).json({
      ok: false,
      error: 'Set SUPABASE_SERVICE_ROLE_KEY before refreshing live bus paddle mappings.',
    });
    return;
  }

  try {
    const result = await refreshLiveBusPaddleMappings();
    res.json({
      ok: true,
      ...result,
      activePaddles: getActivePaddlesForNow().length,
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: String(err.message || 'Unexpected error').slice(0, 500),
    });
  }
}

async function handleGtfsLookup(req, res) {
  const target = parseLookupTarget(req);
  const nowSeconds = parseTimeParamToSeconds(req.query.at);
  const candidateBlock = normalizeBlock(req.query.candidateBlock || req.query.contextBlock || '');
  if (!isGtfsRtConfigured()) {
    res.status(501).json({
      ok: false,
      error: 'Set OCTRANSPO_GTFS_STATIC_URL, OCTRANSPO_GTFS_RT_TRIP_UPDATES_URL, OCTRANSPO_GTFS_RT_VEHICLE_POSITIONS_URL, and OCTRANSPO_GTFS_API_KEY to test GTFS-RT lookup.',
    });
    return;
  }

  try {
    if (target.type === 'bus') {
      if (candidateBlock && !isLikelyBlock(candidateBlock)) {
        res.status(400).json({
          ok: false,
          error: 'candidateBlock format must look like 44-07.',
        });
        return;
      }
      const activePaddles = candidateBlock
        ? [{ block: candidateBlock, serviceDay: normalizeServiceDay(req.query.day) || getOttawaServiceDayKey() }]
        : getActivePaddlesForNow();
      const activePaddlesWithTrips = await Promise.all(activePaddles.map(async (item) => {
        const block = item.block;
        return {
          ...item,
          block,
          trips: await fetchPaddleTripsForBlock(block),
        };
      }));
      const payload = await lookupBusWithGtfsRt(target.value, activePaddlesWithTrips);
      res.json(payload);
      return;
    }

    const rawBlock = target.value;
    if (!validateBlockOrSend(rawBlock, res)) return;
    const canonicalBlock = await resolveCanonicalBlock(rawBlock).catch(() => null);
    const block = canonicalBlock || rawBlock;
    const paddleTrips = await fetchPaddleTripsForBlock(block);
    const payload = await lookupBlockWithGtfsRt(block, paddleTrips, { nowSeconds });
    res.json(payload);
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: String(err.message || 'Unexpected GTFS-RT lookup error').slice(0, 500),
    });
  }
}

async function handleGtfsDebug(req, res) {
  if (!isGtfsRtConfigured()) {
    res.status(501).json({
      ok: false,
      error: 'GTFS-RT is not configured.',
    });
    return;
  }

  try {
    const payload = await debugGtfsState({
      routeId: String(req.query.route || req.query.routeId || '').trim(),
    });
    res.json(payload);
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: String(err.message || 'Unexpected GTFS debug error').slice(0, 500),
    });
  }
}

app.get('/api/track', handleLookup);
app.post('/api/chat', handleChat);
app.get('/api/paddle', handlePaddle);
app.get('/api/shuttle', handleShuttle);
app.get('/api/gtfs-lookup', handleGtfsLookup);
app.get('/api/gtfs-debug', handleGtfsDebug);
app.get('/api/cron/live-bus-paddles', handleRefreshLiveBusPaddles);
app.get('/api/refresh-live-bus-paddles', handleRefreshLiveBusPaddles);
app.get('/refresh-live-bus-paddles', handleRefreshLiveBusPaddles);
app.get('/api/supabase-config', (_req, res) => {
  const enabled = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  res.json({
    ok: true,
    enabled,
    url: enabled ? SUPABASE_URL : '',
    anonKey: enabled ? SUPABASE_ANON_KEY : '',
  });
});
app.get('/api/account-options', (_req, res) => {
  res.json({
    ok: true,
    blocks: getAccountBlockOptions(),
    shuttles: getAccountShuttleOptions(),
  });
});
app.get('/vendor/supabase.js', (_req, res) => {
  res.sendFile(path.join(__dirname, 'node_modules', '@supabase', 'supabase-js', 'dist', 'umd', 'supabase.js'));
});

app.get('/healthz', (_req, res) => {
  res.json({
    ok: true,
    uptimeSec: Math.round(process.uptime()),
    queueDepth: queue.length,
    activeWorkers,
    pendingBlocks: pendingByBlock.size,
    liveOnly: true,
    mode: 'direct-http',
    gtfsWarmup: getGtfsWarmupStatus(),
  });
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

async function startServer() {
  if (isGtfsRtConfigured()) {
    try {
      console.error('Warming GTFS caches before accepting requests...');
      const warmResult = await warmGtfsRtCaches();
      if (warmResult?.ok) {
        console.error(`GTFS warmup complete in ${warmResult.durationMs}ms`);
      }
    } catch (error) {
      console.error(`GTFS warmup failed: ${String(error?.message || error)}`);
    }
  }

  app.listen(PORT, () => {
    console.error(`OC Bus Tracker web app listening on :${PORT}`);
  });
}

if (require.main === module) {
  startServer();
}

module.exports = app;
