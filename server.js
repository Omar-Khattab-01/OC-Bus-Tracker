'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { promisify } = require('util');
const { createClient } = require('@supabase/supabase-js');

if (typeof process.loadEnvFile === 'function' && fs.existsSync(path.join(__dirname, '.env.local'))) {
  process.loadEnvFile(path.join(__dirname, '.env.local'));
}

const {
  captureRealtimeEvidence,
  debugGtfsState,
  getAvailableBlocks: getOfficialGtfsBlocks,
  getStaticCacheStatus,
  getGtfsWarmupStatus,
  isConfigured: isGtfsRtConfigured,
  lookupBlockWithGtfsRt,
  lookupBusPositionWithGtfsRt,
  lookupBusWithGtfsRt,
  warmGtfsRtCaches,
} = require('./lib/gtfs_rt_runtime');
const {
  DEFAULT_MAX_ASSIGNMENT_AGE_MS,
  canUseRetainedAssignmentForPosition,
  isRetainableAssignment,
  maskLocationForScheduledBreak,
  selectNewestAssignments,
} = require('./lib/live_bus_assignment');
const {
  SHUTTLE_DAY_OPTIONS,
  SHUTTLE_DEFINITIONS,
  SHUTTLES_BY_SERVICE_DAY,
} = require('./data/shuttles');
const execFileAsync = promisify(execFile);
const BOOKING_BOARDS_DATA_FILE = path.join(__dirname, 'data', 'booking_boards.json');
const FALL_PDF_SEARCH_INDEX_FILE = path.join(__dirname, 'data', 'fall_pdf_search_index.json');
const FALL_BOOKING_BOARDS_SOURCE_DIR = path.join(__dirname, 'Fall Booking', 'Booking Boards');
const BOOKING_BOARDS_SOURCE_DIR = fs.existsSync(FALL_BOOKING_BOARDS_SOURCE_DIR)
  ? FALL_BOOKING_BOARDS_SOURCE_DIR
  : path.join(__dirname, 'Booking_Boards');
const BOOKING_BOARDS_BUILD_SCRIPT = path.join(__dirname, 'tools', 'build_booking_boards.py');
const PYTHON_BIN = String(process.env.PYTHON_BIN || '').trim();
const PYTHON_VENDOR_DIR = path.join(__dirname, 'vendor', 'python');
const BOOKING_BOARD_ADMIN_TOKEN = String(process.env.BOOKING_BOARD_ADMIN_TOKEN || '').trim();
const INCIDENT_FEEDBACK_ADMIN_EMAIL = String(process.env.INCIDENT_FEEDBACK_ADMIN_EMAIL || 'omar.hosam2000@gmail.com').trim().toLowerCase();
const WHATSAPP_BOOKING_BOARD_TOKEN = String(process.env.WHATSAPP_BOOKING_BOARD_TOKEN || BOOKING_BOARD_ADMIN_TOKEN || '').trim();
const WHATSAPP_ALLOWED_FROM = String(process.env.WHATSAPP_ALLOWED_FROM || '').split(',').map((item) => item.trim()).filter(Boolean);
const WHATSAPP_PUBLIC_WEBHOOK_URL = String(process.env.WHATSAPP_PUBLIC_WEBHOOK_URL || '').trim();
const TWILIO_ACCOUNT_SID = String(process.env.TWILIO_ACCOUNT_SID || '').trim();
const TWILIO_AUTH_TOKEN = String(process.env.TWILIO_AUTH_TOKEN || '').trim();
const TWILIO_API_KEY_SID = String(process.env.TWILIO_API_KEY_SID || '').trim();
const TWILIO_API_KEY_SECRET = String(process.env.TWILIO_API_KEY_SECRET || '').trim();
let bookingBoardsDataCache = null;
let bookingBoardsDataMtimeMs = 0;
let bookingBoardsRuntimeUpdatedAt = '';
let fallPdfSearchIndexCache = null;
let fallPdfSearchIndexMtimeMs = 0;

const PORT = Number(process.env.PORT || 7860);
const RUN_TIMEOUT_MS = Number(process.env.RUN_TIMEOUT_MS || 25000);
const TRACK_CONCURRENCY = Math.max(1, Number(process.env.TRACK_CONCURRENCY || 6));
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').trim();
const SUPABASE_ANON_KEY = String(process.env.SUPABASE_ANON_KEY || '').trim();
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const CRON_SECRET = String(process.env.CRON_SECRET || '').trim();
const LIVE_BUS_ASSIGNMENT_MAX_AGE_MS = Number(
  process.env.LIVE_BUS_ASSIGNMENT_MAX_AGE_MS || DEFAULT_MAX_ASSIGNMENT_AGE_MS
);
const APRIL19_PADDLE_SWITCH_DATE = '2026-04-19';
const SUMMER_WEEKDAY_PADDLE_SWITCH_DATE = '2026-06-29';
const SUMMER_SATURDAY_PADDLE_SWITCH_DATE = '2026-07-04';
const SUMMER_SUNDAY_PADDLE_SWITCH_DATE = '2026-06-28';
const SUMMER_PADDLE_VARIANT_ID = 'june29';
const FALL_PADDLE_VARIANT_ID = 'fall';
const REGULAR_PADDLE_SERVICE_DAYS = ['weekday', 'saturday', 'sunday'];
const SPECIAL_PADDLE_SERVICE_DAYS = ['easter_monday', 'canada_day', 'civic_holiday'];

const pendingByBlock = new Map();
const queue = [];
let activeWorkers = 0;
let paddleIndexCache = null;
const busBlockCache = new Map();
const liveBusPaddleCache = new Map();
let liveBusPaddleRefreshPromise = null;

const BOOKING_BOARD_DAY_OPTIONS = [
  { id: 'weekday', label: 'Weekday' },
  { id: 'saturday', label: 'Saturday' },
  { id: 'sunday', label: 'Sunday' },
  { id: 'special', label: 'Special' },
];

const BOOKING_BOARD_UPLOAD_TARGETS = {
  daily: {
    label: 'Fall Daily Work',
    filename: '2026 Fall Daily boards TRIP .pdf',
    boardIds: ['daily_open_work'],
  },
  spares: {
    label: 'Fall Spare Boards',
    filename: '2026 Fall Daily and weekend spare update.pdf',
    boardIds: ['spares'],
  },
  weekend: {
    label: 'Fall Weekend Work',
    filename: '2026 Fall Weekend boards TRIP.pdf',
    boardIds: ['weekend_boards'],
  },
  days_off_counter: {
    label: 'Fall Days Off Counter',
    filename: '2026 Fall Days off counter  (1).pdf',
    boardIds: ['days_off_counter'],
  },
  vacation_tracker: {
    label: 'Fall Vacation Tracker',
    filename: 'Vacation Tracker Fall 2026 (2).pdf',
    boardIds: ['vacation_tracker'],
  },
  stat: {
    label: 'Fall Stat Work',
    filename: '2026 Fall stat boards TRIP.pdf',
    boardIds: ['stat_work'],
  },
};
const BOOKING_BOARD_PRIMARY_UPLOAD_KEYS = ['daily', 'weekend', 'spares', 'days_off_counter', 'vacation_tracker', 'stat'];
const FLOATING_SPARE_OVERRIDE_DEFINITIONS = [
  { id: 'weekly-floating-spare', title: 'Weekly Floating Spare', limit: 35 },
  { id: 'weekly-pm-floating-spare', title: 'Weekly PM Floating Spare', limit: 8 },
  { id: 'daily-early-early-floating-spare', title: 'Daily Early Early Floating Spare', limit: 20 },
  { id: 'daily-early-floating-spare', title: 'Daily Early Floating Spare', limit: 30 },
];

const adminSupabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
    })
  : null;

const app = express();
app.use(express.json({ limit: '120mb' }));
app.get('/', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'index.html'));
});
app.get('/fall-pdf-viewer', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'fall-pdf-viewer.html'));
});
app.use('/fall-paddles/files', express.static(path.join(__dirname, 'Fall Booking', 'Fall Paddles'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.pdf')) {
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    }
  },
}));
app.use('/fall-paddles/headways', express.static(path.join(__dirname, 'Fall Booking', 'Headways'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.pdf')) {
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    }
  },
}));
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
    }
  },
}));

function normalizeBlock(input) {
  return String(input || '').trim().toUpperCase().replace(/\s*-\s*/g, '-');
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
  if (value === 'canada day' || value === 'canada_day') return 'canada_day';
  if (value === 'civic holiday' || value === 'civic_holiday') return 'civic_holiday';
  return '';
}

function formatServiceDayLabel(day) {
  const value = String(day || '').replace(/_/g, ' ').trim();
  if (!value) return 'Today';
  return value.replace(/\b\w/g, (char) => char.toUpperCase());
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
  if (ottawaIso === '2026-07-01') {
    return 'canada_day';
  }
  if (ottawaIso === '2026-08-03') {
    return 'civic_holiday';
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

function addDays(date, days) {
  return new Date(date.getTime() + days * 24 * 3600 * 1000);
}

function getReferenceDateForServiceDay(serviceDay, referenceDate = new Date()) {
  if (serviceDay === 'canada_day') return new Date('2026-07-01T12:00:00-04:00');
  if (serviceDay === 'civic_holiday') return new Date('2026-08-03T12:00:00-04:00');
  if (serviceDay === 'easter_monday') return new Date('2026-04-06T12:00:00-04:00');
  for (let offset = 0; offset < 14; offset += 1) {
    const candidate = addDays(referenceDate, offset);
    if (getServiceDayKeyForDate(candidate) === serviceDay) {
      return candidate;
    }
  }
  return referenceDate;
}

function isEasterMondayOptionVisible(referenceDate = new Date()) {
  const ottawaIso = getOttawaDateString(referenceDate);
  return ottawaIso === '2026-04-05' || ottawaIso === '2026-04-06';
}

function isCanadaDayOptionVisible(referenceDate = new Date()) {
  const ottawaIso = getOttawaDateString(referenceDate);
  return ottawaIso === '2026-06-30' || ottawaIso === '2026-07-01';
}

function isCivicHolidayOptionVisible(referenceDate = new Date()) {
  const ottawaIso = getOttawaDateString(referenceDate);
  return ottawaIso === '2026-08-02' || ottawaIso === '2026-08-03';
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

function getShuttleForToday(id, serviceDay = getOttawaServiceDayKey()) {
  const definition = SHUTTLE_DEFINITIONS[id];
  if (!definition) return null;

  const resolvedDefinition = definition.tripsByServiceDay
    ? {
        ...definition,
        trips: definition.tripsByServiceDay[serviceDay] || definition.tripsByServiceDay.weekday || definition.trips,
      }
    : definition;

  return {
    ...resolvedDefinition,
    trips: Array.isArray(resolvedDefinition.trips)
      ? buildExplicitTrips(resolvedDefinition)
      : buildPatternTrips(resolvedDefinition),
  };
}

function getAvailableShuttlesForDay(serviceDay = getOttawaServiceDayKey()) {
  const ids = SHUTTLES_BY_SERVICE_DAY[serviceDay] || [];
  return ids
    .map((id) => getShuttleForToday(id, serviceDay))
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
  const trips = Array.isArray(run?.trips) ? run.trips.filter(isUsablePaddleTrip) : [];
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

function buildBreakAwareRunTrips(run) {
  const trips = Array.isArray(run?.trips) ? run.trips.filter(isUsablePaddleTrip) : [];
  const enriched = [];
  let dayOffset = 0;
  let previousStart = null;

  for (const trip of trips) {
    const rawStart = timeToSeconds(trip.start_time);
    const rawEnd = timeToSeconds(trip.end_time);
    if (rawStart === null) continue;

    if (previousStart !== null && rawStart < previousStart) {
      dayOffset += 24 * 3600;
    }

    const startSeconds = rawStart + dayOffset;
    let endSeconds = (rawEnd === null ? rawStart : rawEnd) + dayOffset;
    if (endSeconds < startSeconds) {
      endSeconds += 24 * 3600;
    }

    enriched.push({
      ...trip,
      trip_number: Number(trip.trip_number ?? trip.tripNumber ?? 0) || null,
      timelineStartSeconds: startSeconds,
      timelineEndSeconds: endSeconds,
      breakAfterMinutes: null,
      nextTripNumber: null,
      nextTripRoute: '',
      nextTripHeadsign: '',
      nextTripStartTime: '',
      nextTripStartStop: '',
      splitBreak: false,
    });

    previousStart = rawStart;
  }

  for (let index = 0; index < enriched.length; index += 1) {
    const current = enriched[index];
    const next = enriched[index + 1] || null;
    if (!next) continue;
    const breakAfterMinutes = Math.max(0, Math.round((next.timelineStartSeconds - current.timelineEndSeconds) / 60));
    current.breakAfterMinutes = breakAfterMinutes;
    current.nextTripNumber = next.trip_number ?? null;
    current.nextTripRoute = String(next.route || '');
    current.nextTripHeadsign = String(next.headsign || '');
    current.nextTripStartTime = String(next.start_time || '');
    current.nextTripStartStop = String(next.start_stop || '');
    current.splitBreak = breakAfterMinutes >= 90;
  }

  return enriched;
}

function findActiveBreakInRun(run, compareSeconds) {
  const trips = buildBreakAwareRunTrips(run);
  for (const trip of trips) {
    const breakAfterMinutes = Number(trip.breakAfterMinutes);
    if (!Number.isFinite(breakAfterMinutes) || breakAfterMinutes <= 0) continue;
    const breakStart = Number(trip.timelineEndSeconds);
    const breakEnd = breakStart + breakAfterMinutes * 60;
    if (breakStart <= compareSeconds && compareSeconds < breakEnd) {
      return {
        afterTripNumber: trip.trip_number,
        breakAfterMinutes,
        splitBreak: Boolean(trip.splitBreak),
        startedAt: String(trip.end_time || ''),
        endsAt: String(trip.nextTripStartTime || ''),
        breakStartSeconds: breakStart,
        breakEndSeconds: breakEnd,
        remainingMinutes: Math.max(0, Math.ceil((breakEnd - compareSeconds) / 60)),
        nextTripNumber: trip.nextTripNumber,
        nextTripRoute: trip.nextTripRoute,
        nextTripHeadsign: trip.nextTripHeadsign,
        nextTripStartTime: trip.nextTripStartTime,
        nextTripStartStop: trip.nextTripStartStop,
      };
    }
  }
  return null;
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

function getTodayBoardPaddlesForNow() {
  const index = loadPaddleIndex();
  const now = new Date();
  const currentSeconds = getOttawaNowSeconds();
  const currentDayKey = getServiceDayKeyForDate(now);
  const previousDate = new Date(now.getTime() - 24 * 3600 * 1000);
  const previousDayKey = getServiceDayKeyForDate(previousDate);
  const paddleIds = new Set([
    ...Object.keys(index?.service_days?.[currentDayKey] || {}),
    ...Object.keys(index?.service_days?.[previousDayKey] || {}),
  ]);
  const results = [];

  for (const paddleId of paddleIds) {
    const resolved = resolvePaddleRunForCurrentContext(paddleId);
    if (!resolved?.run) continue;

    const compareSeconds = resolved.carryover ? currentSeconds + 24 * 3600 : currentSeconds;
    const activeTrip = resolved.activeTrip || null;
    const activeBreak = activeTrip ? null : findActiveBreakInRun(resolved.run, compareSeconds);
    if (!activeTrip && !activeBreak) continue;

    results.push({
      block: paddleIdToBlockLabel(paddleId),
      paddleId,
      serviceDay: resolved.serviceDay,
      variantId: resolved.variantId || resolved.run.variant_id || null,
      variantLabel: resolved.variantLabel || resolved.run.variant_label || null,
      displayVariantLabel: getPaddleDisplayVariantLabel(
        now,
        resolved.variantId || resolved.run.variant_id || null,
        resolved.serviceDay
      ),
      carryover: Boolean(resolved.carryover),
      sourceId: resolved.run.source_id || null,
      sourceLabel: resolved.run.source_label || null,
      effective: resolved.run.effective || null,
      garage: resolved.run.garage || null,
      signOn: resolved.run.sign_on || null,
      routes: Array.isArray(resolved.run.routes) ? resolved.run.routes : [],
      busType: resolved.run.bus_type || null,
      activeTrip,
      activeBreak,
    });
  }

  return results.sort((a, b) => {
    const aRemaining = a.activeBreak?.remainingMinutes ?? (a.activeTrip ? Math.max(0, Math.ceil((a.activeTrip.end - (a.carryover ? currentSeconds + 24 * 3600 : currentSeconds)) / 60)) : Number.MAX_SAFE_INTEGER);
    const bRemaining = b.activeBreak?.remainingMinutes ?? (b.activeTrip ? Math.max(0, Math.ceil((b.activeTrip.end - (b.carryover ? currentSeconds + 24 * 3600 : currentSeconds)) / 60)) : Number.MAX_SAFE_INTEGER);
    return aRemaining - bRemaining ||
      a.block.localeCompare(b.block, undefined, { numeric: true });
  });
}

function addLiveBusMappingToBoardIndex(indexByBlock, busNumber, value) {
  const normalizedBus = String(busNumber || '').trim();
  if (!normalizedBus || !value?.block || !isLiveBusMappingFresh(value)) return;

  const block = String(value.block || '').trim();
  if (!block) return;

  const existing = indexByBlock.get(block) || new Map();
  const entry = {
    busNumber: normalizedBus,
    block,
    paddleId: String(value.paddleId || '').trim(),
    serviceDay: String(value.serviceDay || '').trim(),
    route: String(value.route || '').trim(),
    headsign: String(value.headsign || '').trim(),
    tripNumber: value.tripNumber == null ? null : Number(value.tripNumber) || null,
    startTime: String(value.startTime || '').trim(),
    endTime: String(value.endTime || '').trim(),
    verifiedAt: String(value.verifiedAt || '').trim(),
  };
  const newestExistingTimestamp = Math.max(
    ...Array.from(existing.values()).map((item) => Date.parse(item.verifiedAt) || 0),
    0
  );
  const entryTimestamp = Date.parse(entry.verifiedAt) || 0;
  if (entryTimestamp < newestExistingTimestamp) return;
  if (entryTimestamp > newestExistingTimestamp) existing.clear();
  existing.set(normalizedBus, entry);
  indexByBlock.set(block, existing);
}

async function getLiveBusMappingsByBlock() {
  const indexByBlock = new Map();

  for (const [busNumber, value] of liveBusPaddleCache.entries()) {
    addLiveBusMappingToBoardIndex(indexByBlock, busNumber, value);
  }

  if (adminSupabase) {
    const cutoffIso = new Date(Date.now() - LIVE_BUS_ASSIGNMENT_MAX_AGE_MS).toISOString();
    const { data, error } = await adminSupabase
      .from('live_bus_paddles')
      .select('bus_number, block, paddle_id, service_day, route, trip_number, headsign, start_time, end_time, verified_at')
      .gte('verified_at', cutoffIso);

    if (error) {
      throw error;
    }

    for (const row of data || []) {
      addLiveBusMappingToBoardIndex(indexByBlock, row.bus_number, {
        block: row.block,
        paddleId: row.paddle_id,
        serviceDay: row.service_day,
        route: row.route,
        tripNumber: row.trip_number,
        headsign: row.headsign,
        startTime: row.start_time,
        endTime: row.end_time,
        verifiedAt: row.verified_at,
      });
    }
  }

  return new Map(
    Array.from(indexByBlock.entries()).map(([block, buses]) => [block, Array.from(buses.values())])
  );
}

function countLiveBusesInBoardMap(liveMappingsByBlock) {
  return Array.from(liveMappingsByBlock.values()).reduce((total, buses) => total + (Array.isArray(buses) ? buses.length : 0), 0);
}

function buildBoardMapFromLiveMappings(mappings) {
  const indexByBlock = new Map();
  for (const [busNumber, value] of mappings.entries()) {
    addLiveBusMappingToBoardIndex(indexByBlock, busNumber, value);
  }
  return new Map(
    Array.from(indexByBlock.entries()).map(([block, buses]) => [block, Array.from(buses.values())])
  );
}

async function buildTodayBoardPayload() {
  const now = new Date();
  const nowSeconds = getOttawaNowSeconds();
  let liveMappingsByBlock = await getLiveBusMappingsByBlock().catch(() => new Map());
  if (countLiveBusesInBoardMap(liveMappingsByBlock) === 0 && isGtfsRtConfigured()) {
    const rebuiltMappings = await buildLiveBusPaddleMappings().catch(() => null);
    if (rebuiltMappings instanceof Map && rebuiltMappings.size) {
      liveMappingsByBlock = buildBoardMapFromLiveMappings(rebuiltMappings);
      if (adminSupabase) {
        await persistLiveBusPaddleMappings(rebuiltMappings).catch(() => null);
      }
    }
  }
  const activePaddles = getTodayBoardPaddlesForNow().map((entry) => {
    const compareSeconds = entry.carryover ? nowSeconds + 24 * 3600 : nowSeconds;
    const buses = liveMappingsByBlock.get(entry.block) || [];
    const activeTrip = entry.activeTrip || null;
    const activeBreak = entry.activeBreak || null;
    const primaryBus = buses[0] || null;
    const kind = activeTrip ? 'trip' : 'break';
    const minutesRemaining = activeBreak
      ? activeBreak.remainingMinutes
      : Math.max(0, Math.ceil((activeTrip.end - compareSeconds) / 60));
    const busNumbers = buses
      .map((bus) => String(bus?.busNumber || '').trim())
      .filter(Boolean);
    const route = String(activeTrip?.route || activeBreak?.nextTripRoute || '').trim();
    const tripNumber = activeTrip?.tripNumber ?? activeBreak?.nextTripNumber ?? null;
    const location = String(primaryBus?.locationText || '').trim()
      || (activeBreak
        ? `Break until ${activeBreak.endsAt || '--:--'}`
        : String(activeTrip?.headsign || '').trim()
          || 'Live location unavailable');
    const statusLabel = activeTrip
      ? `Trip ${activeTrip.tripNumber || '?'}${route ? ` on route ${route}` : ''}`
      : `${activeBreak.splitBreak ? 'Split break' : 'Break'} until ${activeBreak.endsAt || '--:--'}`;

    return {
      block: entry.block,
      paddleId: entry.paddleId,
      serviceDay: entry.serviceDay,
      variantId: entry.variantId || '',
      status: kind,
      statusLabel,
      minutesRemaining,
      route,
      tripNumber,
      busNumbers,
      primaryBusNumber: busNumbers[0] || '',
      hasLiveBus: busNumbers.length > 0,
      location,
      verifiedAt: String(primaryBus?.verifiedAt || '').trim(),
      open: {
        block: entry.block,
        serviceDay: entry.serviceDay,
        variantId: entry.variantId || '',
      },
    };
  });
  const trackedBuses = new Set(activePaddles.flatMap((item) => item.busNumbers));
  const liveMatchedCount = activePaddles.filter((item) => item.hasLiveBus).length;
  const breakCount = activePaddles.filter((item) => item.status === 'break').length;

  return {
    ok: true,
    mode: 'today-board',
    generatedAt: now.toISOString(),
    serviceDay: getOttawaServiceDayKey(),
    serviceLabel: formatServiceDayLabel(getOttawaServiceDayKey()),
    summary: {
      activePaddles: activePaddles.length,
      liveMatched: liveMatchedCount,
      noLiveMatch: Math.max(0, activePaddles.length - liveMatchedCount),
      trackedBuses: trackedBuses.size,
      activeBreaks: breakCount,
    },
    activePaddles,
  };
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
  if (!value?.block) return false;
  const status = getPublicLocationStatusForBlock(value.block);
  return isRetainableAssignment(value, {
    maxAgeMs: LIVE_BUS_ASSIGNMENT_MAX_AGE_MS,
    paddleCarryover: Boolean(status?.paddle?.carryover),
    afterFinalTrip: !status?.paddle || status.afterFinalTrip,
  });
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

  const staleBefore = new Date(Date.now() - LIVE_BUS_ASSIGNMENT_MAX_AGE_MS).toISOString();
  const { error: deleteError } = await adminSupabase
    .from('live_bus_paddles')
    .delete()
    .lt('verified_at', staleBefore);
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

  const cutoffIso = new Date(Date.now() - LIVE_BUS_ASSIGNMENT_MAX_AGE_MS).toISOString();
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
  if (!isLiveBusMappingFresh(mapping)) return null;
  rememberLiveBusPaddleMapping(normalizedBus, mapping);
  return mapping;
}

async function getStoredLiveBusPaddleMappingsForBlock(block) {
  const normalizedBlock = normalizeBlock(block);
  if (!normalizedBlock) return [];
  const candidatesByBus = new Map();

  for (const [busNumber, value] of liveBusPaddleCache.entries()) {
    if (normalizeBlock(value?.block) !== normalizedBlock) continue;
    candidatesByBus.set(busNumber, { busNumber, ...value, block: normalizedBlock });
  }

  if (adminSupabase) {
    const cutoffIso = new Date(Date.now() - LIVE_BUS_ASSIGNMENT_MAX_AGE_MS).toISOString();
    const { data, error } = await adminSupabase
      .from('live_bus_paddles')
      .select('bus_number, block, paddle_id, service_day, route, trip_number, headsign, start_time, end_time, verified_at')
      .eq('block', normalizedBlock)
      .gte('verified_at', cutoffIso)
      .order('verified_at', { ascending: false });
    if (error) throw error;

    for (const row of data || []) {
      const busNumber = String(row.bus_number || '').trim();
      if (!busNumber) continue;
      const mapping = {
        busNumber,
        block: normalizedBlock,
        paddleId: row.paddle_id,
        serviceDay: row.service_day,
        route: row.route,
        tripNumber: row.trip_number,
        headsign: row.headsign,
        startTime: row.start_time,
        endTime: row.end_time,
        verifiedAt: row.verified_at,
      };
      const cached = candidatesByBus.get(busNumber);
      if (!cached || Date.parse(mapping.verifiedAt) > Date.parse(cached.verifiedAt)) {
        candidatesByBus.set(busNumber, mapping);
      }
    }
  }

  const status = getPublicLocationStatusForBlock(normalizedBlock);
  const selected = selectNewestAssignments(Array.from(candidatesByBus.values()), {
    maxAgeMs: LIVE_BUS_ASSIGNMENT_MAX_AGE_MS,
    paddleCarryover: Boolean(status?.paddle?.carryover),
    afterFinalTrip: !status?.paddle || status.afterFinalTrip,
  });
  for (const mapping of selected) {
    rememberLiveBusPaddleMapping(mapping.busNumber, mapping);
  }
  return selected;
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

  const shuttle = getShuttleForToday(id, serviceDay);
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

function resolveShuttleCatalogDay(requestedDay) {
  const text = String(requestedDay || '').trim().toLowerCase();
  if (!text || text === 'today' || text === 'current') {
    return getOttawaServiceDayKey();
  }
  return normalizeServiceDay(text) || getOttawaServiceDayKey();
}

function buildShuttleCatalogResponse(requestedDay) {
  const serviceDay = resolveShuttleCatalogDay(requestedDay);
  const currentServiceDay = getOttawaServiceDayKey();
  const shuttles = getAvailableShuttlesForDay(serviceDay).map((shuttle) => {
    const details = buildShuttleResponse(shuttle.id, serviceDay);
    return {
      id: shuttle.id,
      route: shuttle.route,
      name: shuttle.name,
      serviceDay,
      isLiveDay: Boolean(details?.isLiveDay),
      sourceLabel: shuttle.sourceLabel,
      tripCount: Array.isArray(details?.trips) ? details.trips.length : 0,
      nextStop: details?.nextStop || null,
      liveSummary: details?.nextStop?.summary || '',
    };
  });

  return {
    ok: true,
    mode: 'shuttle-catalog',
    serviceDay,
    currentServiceDay,
    serviceDayOptions: ['today', ...SHUTTLE_DAY_OPTIONS],
    shuttles,
    generatedAt: new Date().toISOString(),
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

function isSummerPaddleVariantActive(referenceDate = new Date()) {
  return getOttawaDateString(referenceDate) >= SUMMER_WEEKDAY_PADDLE_SWITCH_DATE;
}

function isSummerPaddleVariantActiveForServiceDay(referenceDate = new Date(), serviceDay = '') {
  const ottawaDate = getOttawaDateString(referenceDate);
  if (serviceDay === 'sunday') return ottawaDate >= SUMMER_SUNDAY_PADDLE_SWITCH_DATE;
  if (serviceDay === 'saturday') return ottawaDate >= SUMMER_SATURDAY_PADDLE_SWITCH_DATE;
  return ottawaDate >= SUMMER_WEEKDAY_PADDLE_SWITCH_DATE;
}

function getDefaultPaddleVariantIdForDate(referenceDate = new Date(), serviceDay = '') {
  if (serviceDay === 'easter_monday') return 'current';
  if (serviceDay === 'canada_day' || serviceDay === 'civic_holiday') return SUMMER_PADDLE_VARIANT_ID;
  if (isSummerPaddleVariantActiveForServiceDay(referenceDate, serviceDay)) return SUMMER_PADDLE_VARIANT_ID;
  if (isApril19PaddleVariantActive(referenceDate)) return 'april19';
  return 'current';
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

function getPinnedPaddleRunForDay(serviceDay, paddleId, variantId) {
  if (!serviceDay || !paddleId || !variantId) return null;
  return getPaddleRunForVariant(variantId, serviceDay, paddleId);
}

function getPinnedVariantServiceDaysForPaddle(paddleId, variantId) {
  if (!paddleId || !variantId) return [];

  const currentServiceDay = getOttawaServiceDayKey();
  const orderedDays = [currentServiceDay, ...REGULAR_PADDLE_SERVICE_DAYS, ...SPECIAL_PADDLE_SERVICE_DAYS]
    .filter((value, index, list) => [...REGULAR_PADDLE_SERVICE_DAYS, ...SPECIAL_PADDLE_SERVICE_DAYS].includes(value) && list.indexOf(value) === index);

  return orderedDays.filter((serviceDay) => getPinnedPaddleRunForDay(serviceDay, paddleId, variantId));
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
    ? formatOttawaDate(getPreviousOttawaDate(new Date()))
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
  if (preferredVariantId === SUMMER_PADDLE_VARIANT_ID) return null;

  if (preferredVariantId !== 'current') {
    const fallbackCurrent = getPaddleRunForVariant('current', serviceDay, paddleId);
    if (fallbackCurrent) return fallbackCurrent;
  }
  if (preferredVariantId !== 'april19') {
    const fallbackApril19 = getPaddleRunForVariant('april19', serviceDay, paddleId);
    if (fallbackApril19) return fallbackApril19;
  }
  if (preferredVariantId !== SUMMER_PADDLE_VARIANT_ID) {
    const fallbackSummer = getPaddleRunForVariant(SUMMER_PADDLE_VARIANT_ID, serviceDay, paddleId);
    if (fallbackSummer) return fallbackSummer;
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
    } else if (variantId !== currentDefaultVariantId && !REGULAR_PADDLE_SERVICE_DAYS.includes(serviceDay) && serviceDay !== 'easter_monday') {
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

  for (const serviceDay of REGULAR_PADDLE_SERVICE_DAYS) {
    const serviceReferenceDate = getReferenceDateForServiceDay(serviceDay, referenceDate);
    if (beforeSwitch) {
      addOption(serviceDay, getPaddleRunForVariant('current', serviceDay, paddleId), 'current');
      addOption(serviceDay, getPaddleRunForVariant('april19', serviceDay, paddleId), 'april19');
    } else {
      const preferredRun = getPaddleRunForDay(serviceDay, paddleId, {
        referenceDate: serviceReferenceDate,
        variantId: getDefaultPaddleVariantIdForDate(serviceReferenceDate, serviceDay),
      });
      addOption(serviceDay, preferredRun, preferredRun?.variant_id || getDefaultPaddleVariantIdForDate(serviceReferenceDate, serviceDay));
    }
  }

  if (isEasterMondayOptionVisible(referenceDate)) {
    addOption('easter_monday', getPaddleRunForDay('easter_monday', paddleId, {
      referenceDate,
      variantId: 'current',
    }), 'current');
  }
  if (isCanadaDayOptionVisible(referenceDate)) {
    addOption('canada_day', getPaddleRunForDay('canada_day', paddleId, {
      referenceDate,
      variantId: SUMMER_PADDLE_VARIANT_ID,
    }), SUMMER_PADDLE_VARIANT_ID);
  }
  if (isCivicHolidayOptionVisible(referenceDate)) {
    addOption('civic_holiday', getPaddleRunForDay('civic_holiday', paddleId, {
      referenceDate,
      variantId: SUMMER_PADDLE_VARIANT_ID,
    }), SUMMER_PADDLE_VARIANT_ID);
  }

  const currentServiceDay = getOttawaServiceDayKey();
  const baseOrder = new Map([
    ['weekday', 0],
    ['saturday', 1],
    ['sunday', 2],
    ['easter_monday', 3],
    ['canada_day', 4],
    ['civic_holiday', 5],
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

function formatOttawaDate(date = new Date()) {
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
  const explicitReferenceDate = explicitDay ? getReferenceDateForServiceDay(explicitDay, now) : now;
  const currentResolved = resolvePaddleRunForCurrentContext(paddleId);
  const resolvedExplicitVariantId = explicitDay
    ? (explicitVariantId || getDefaultPaddleVariantIdForDate(explicitReferenceDate, explicitDay))
    : '';
  const explicitRun = explicitDay
    ? getPaddleRunForDay(explicitDay, paddleId, { referenceDate: explicitReferenceDate, variantId: resolvedExplicitVariantId })
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
  const breakAwareTrips = buildBreakAwareRunTrips(run);
  const isLiveContext = !explicitDay || (
    currentResolved?.serviceDay === serviceDay &&
    currentResolved?.variantId === (variantId || run.variant_id || null)
  );
  const compareSeconds = carryover ? getOttawaNowSeconds() + 24 * 3600 : getOttawaNowSeconds();
  const activeBreak = isLiveContext && !resolved.activeTrip ? findActiveBreakInRun(run, compareSeconds) : null;

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
    activeBreak,
    currentServiceDay: currentResolved?.serviceDay || null,
    isLiveDay: Boolean(isLiveContext && (resolved.activeTrip || activeBreak)),
    trips: breakAwareTrips,
  };
}

function buildPinnedVariantPaddleResponse(block, variantId, requestedDay = '') {
  const paddleId = blockToPaddleId(block);
  if (!paddleId || !variantId) return null;

  const explicitServiceDay = normalizeServiceDay(requestedDay);
  const availableServiceDays = getPinnedVariantServiceDaysForPaddle(paddleId, variantId);
  const serviceDay = explicitServiceDay || availableServiceDays[0] || '';
  if (!serviceDay || ![...REGULAR_PADDLE_SERVICE_DAYS, ...SPECIAL_PADDLE_SERVICE_DAYS].includes(serviceDay)) {
    return null;
  }

  const run = getPinnedPaddleRunForDay(serviceDay, paddleId, variantId);
  if (!run) return null;

  const compareSeconds = serviceDay === getOttawaServiceDayKey() ? getOttawaNowSeconds() : null;
  const activeTrip = compareSeconds === null ? null : findActiveTripInRun(run, compareSeconds);
  const activeBreak = compareSeconds === null || activeTrip ? null : findActiveBreakInRun(run, compareSeconds);

  return {
    block: String(block),
    paddleId,
    serviceDay,
    variantId,
    variantLabel: run.variant_label || getPaddleVariantMeta(variantId)?.label || null,
    displayVariantLabel: run.variant_label || getPaddleVariantMeta(variantId)?.label || null,
    carryover: false,
    sourceId: run.source_id || null,
    sourceLabel: run.source_label || null,
    effective: run.effective || null,
    garage: run.garage || null,
    signOn: run.sign_on || null,
    routes: Array.isArray(run.routes) ? run.routes : [],
    busType: run.bus_type || null,
    activeTrip,
    activeBreak,
    currentServiceDay: getOttawaServiceDayKey(),
    isLiveDay: Boolean(serviceDay === getOttawaServiceDayKey() && (activeTrip || activeBreak)),
    trips: buildBreakAwareRunTrips(run),
  };
}

function getPinnedVariantPaddleOptionsForBlock(block, variantId) {
  const paddleId = blockToPaddleId(block);
  if (!paddleId || !variantId) return [];

  const options = getPinnedVariantServiceDaysForPaddle(paddleId, variantId)
    .map((serviceDay) => {
      const run = getPinnedPaddleRunForDay(serviceDay, paddleId, variantId);
      if (!run) return null;
      return {
        serviceDay,
        sourceId: run.source_id || null,
        sourceLabel: run.source_label || null,
        effective: run.effective || null,
        variantId,
        variantLabel: run.variant_label || getPaddleVariantMeta(variantId)?.label || null,
        displayVariantLabel: run.variant_label || getPaddleVariantMeta(variantId)?.label || null,
        buttonLabel: `${formatServiceDayLabel(serviceDay)} Paddle`,
      };
    })
    .filter(Boolean);

  const currentServiceDay = getOttawaServiceDayKey();
  const baseOrder = new Map([
    ['weekday', 0],
    ['saturday', 1],
    ['sunday', 2],
  ]);

  return options.sort((a, b) => {
    if (a.serviceDay === currentServiceDay && b.serviceDay !== currentServiceDay) return -1;
    if (b.serviceDay === currentServiceDay && a.serviceDay !== currentServiceDay) return 1;
    return (baseOrder.get(a.serviceDay) ?? 99) - (baseOrder.get(b.serviceDay) ?? 99);
  });
}

function formatSummerBookingReply(paddle) {
  if (!paddle) {
    return 'No June 29 summer paddle was found for that block.';
  }

  const blockMatch = String(paddle.block || '').match(/^(\d{1,3})-(\d{1,3})$/);
  const displayBlock = blockMatch
    ? `${Number(blockMatch[1])}-${Number(blockMatch[2])}`
    : String(paddle.block || '').trim();
  return `Here are the paddles for ${displayBlock} for the summer.`;
}

function formatFallBookingReply(paddle) {
  if (!paddle) {
    return 'No fall booking paddle was found for that block.';
  }

  const blockMatch = String(paddle.block || '').match(/^(\d{1,3})-(\d{1,3})$/);
  const displayBlock = blockMatch
    ? `${Number(blockMatch[1])}-${Number(blockMatch[2])}`
    : String(paddle.block || '').trim();
  return `Here are the paddles for ${displayBlock} for the fall booking.`;
}

function formatCanadaDayPaddleReply(paddle) {
  if (!paddle) {
    return 'No Canada Day paddle was found for that block.';
  }

  const blockMatch = String(paddle.block || '').match(/^(\d{1,3})-(\d{1,3})$/);
  const displayBlock = blockMatch
    ? `${Number(blockMatch[1])}-${Number(blockMatch[2])}`
    : String(paddle.block || '').trim();
  return `Here is the Canada Day paddle for ${displayBlock}.`;
}

function formatCivicHolidayPaddleReply(paddle) {
  if (!paddle) {
    return 'No Civic Holiday paddle was found for that block.';
  }

  const blockMatch = String(paddle.block || '').match(/^(\d{1,3})-(\d{1,3})$/);
  const displayBlock = blockMatch
    ? `${Number(blockMatch[1])}-${Number(blockMatch[2])}`
    : String(paddle.block || '').trim();
  return `Here is the Civic Holiday paddle for ${displayBlock}.`;
}

function normalizeBookingBoardTaken(entry = {}) {
  if (entry.taken === true) return true;
  if (Array.isArray(entry.pieces) && entry.pieces.some((piece) => piece?.taken === true)) return true;
  return false;
}

function buildBookingBoardPieceSummary(piece = {}, entry = {}) {
  const block = normalizeBlock(piece.block || '');
  const routeLabel = String(piece.routeLabel || '').trim();
  const startTime = String(piece.startTime || '').trim();
  const endTime = String(piece.endTime || '').trim();
  const from = String(piece.from || '').trim();
  const to = String(piece.to || '').trim();
  const payTime = String(piece.payTime || '').trim();
  return {
    pieceId: String(piece.pieceId || '').trim(),
    block,
    routeLabel,
    from,
    to,
    startTime,
    endTime,
    payTime,
    taken: piece?.taken === true || entry.taken === true,
    highlightWindow: block && startTime && endTime
      ? {
          block,
          startTime,
          endTime,
        }
      : null,
  };
}

function buildBookingBoardEntrySummary(entry = {}, board = {}) {
  const pieces = Array.isArray(entry.pieces) ? entry.pieces.map((piece) => buildBookingBoardPieceSummary(piece, entry)) : [];
  const uniqueBlocks = [...new Set(pieces.map((piece) => piece.block).filter(Boolean))];
  const title = String(entry.title || '').trim() || 'Booking board shift';
  const sat1Taken = Boolean(entry.sat1Taken);
  const sat2Taken = Boolean(entry.sat2Taken);
  const sun1Taken = Boolean(entry.sun1Taken);
  const sun2Taken = Boolean(entry.sun2Taken);
  return {
    id: String(entry.id || '').trim(),
    title,
    serviceDay: String(entry.serviceDay || board.serviceDay || '').trim(),
    boardPage: Number(entry.boardPage || 0) || null,
    availabilityStart: String(entry.availabilityStart || '').trim(),
    availabilityEnd: String(entry.availabilityEnd || '').trim(),
    taken: normalizeBookingBoardTaken(entry),
    sourcePdf: String(entry.sourcePdf || board.sourcePdf || '').trim(),
    workSection: String(entry.workSection || '').trim(),
    holidayKey: String(entry.holidayKey || '').trim(),
    holidayLabel: String(entry.holidayLabel || '').trim(),
    pieces,
    uniqueBlocks,
    pieceCount: pieces.length,
    shiftId: String(entry.shiftId || '').trim(),
    sat1Taken,
    sat2Taken,
    sun1Taken,
    sun2Taken,
  };
}

function isFullyTakenWeekendEntry(entry = {}) {
  if (entry.serviceDay === 'saturday') return Boolean(entry.sat1Taken && entry.sat2Taken);
  if (entry.serviceDay === 'sunday') return Boolean(entry.sun1Taken && entry.sun2Taken);
  return false;
}

function buildSpareBoardSectionSummary(section = {}) {
  const garages = Array.isArray(section.garages) ? section.garages : [];
  const garageSummaries = garages.map((garage) => {
    const slots = Array.isArray(garage.slots) ? garage.slots : [];
    const slotSummaries = slots.map((slot) => ({
      onTime: String(slot.onTime || '').trim(),
      limit: Number(slot.limit || 0) || 0,
      booked: Number(slot.booked || 0) || 0,
      available: Number(slot.available || 0) || 0,
    }));
    const availableTotal = slotSummaries.reduce((sum, slot) => sum + slot.available, 0);
    const bookedTotal = slotSummaries.reduce((sum, slot) => sum + slot.booked, 0);
    const limitTotal = slotSummaries.reduce((sum, slot) => sum + slot.limit, 0);
    const openSlots = slotSummaries.filter((slot) => slot.available > 0);
    return {
      name: String(garage.name || '').trim() || 'All locations',
      slots: slotSummaries,
      availableTotal,
      bookedTotal,
      limitTotal,
      openSlots,
    };
  });
  return {
    id: String(section.id || '').trim(),
    title: String(section.title || '').trim() || 'Spare board',
    page: Number(section.page || 0) || null,
    group: String(section.group || '').trim(),
    kind: String(section.kind || '').trim(),
    holidayKey: String(section.holidayKey || '').trim(),
    holidayLabel: String(section.holidayLabel || '').trim(),
    garages: garageSummaries,
    availableTotal: garageSummaries.reduce((sum, garage) => sum + garage.availableTotal, 0),
    bookedTotal: garageSummaries.reduce((sum, garage) => sum + garage.bookedTotal, 0),
    limitTotal: garageSummaries.reduce((sum, garage) => sum + garage.limitTotal, 0),
    openSlotCount: garageSummaries.reduce((sum, garage) => sum + garage.openSlots.length, 0),
  };
}

function buildDailySpareSummaryRow(row = {}) {
  return {
    id: String(row.id || '').trim(),
    title: String(row.title || '').trim() || 'Spare summary',
    limit: Number(row.limit || 0) || 0,
    booked: Number(row.booked || 0) || 0,
    available: Number(row.available || 0) || 0,
  };
}

function numericDaysOffCount(value) {
  if (String(value || '').trim().toLowerCase() === 'closed') return 0;
  const count = Number(value || 0);
  return Number.isFinite(count) ? count : 0;
}

function displayDaysOffRemaining(value) {
  if (String(value || '').trim().toLowerCase() === 'closed') return 'Closed';
  const count = numericDaysOffCount(value);
  return count === 0 ? 'Closed' : count;
}

function buildDaysOffCounterSummary(counter = {}) {
  const rows = Array.isArray(counter.rows) ? counter.rows.map((row) => ({
    day: String(row.day || '').trim(),
    week: String(row.week || '').trim(),
    label: String(row.label || '').trim(),
    dateRange: String(row.dateRange || '').trim(),
    total: Number(row.total || 0) || 0,
    booked: Number(row.booked || 0) || 0,
    remaining: displayDaysOffRemaining(row.remaining),
  })) : [];
  const calculatedRemaining = rows.reduce((sum, row) => sum + numericDaysOffCount(row.remaining), 0);
  return {
    id: String(counter.id || '').trim(),
    title: String(counter.title || '').trim() || 'Counter',
    rows,
    total: Number(counter.total || 0) || rows.reduce((sum, row) => sum + row.total, 0),
    booked: Number(counter.booked || 0) || rows.reduce((sum, row) => sum + row.booked, 0),
    remaining: Number(counter.remaining || 0) || calculatedRemaining,
  };
}

function loadBookingBoardsData() {
  const stat = fs.statSync(BOOKING_BOARDS_DATA_FILE);
  if (bookingBoardsDataCache && bookingBoardsDataMtimeMs === -1) {
    return bookingBoardsDataCache;
  }
  if (bookingBoardsDataCache && bookingBoardsDataMtimeMs === stat.mtimeMs) {
    return bookingBoardsDataCache;
  }
  bookingBoardsDataCache = JSON.parse(fs.readFileSync(BOOKING_BOARDS_DATA_FILE, 'utf8'));
  bookingBoardsDataMtimeMs = stat.mtimeMs;
  return bookingBoardsDataCache;
}

function loadFallPdfSearchIndex() {
  const stat = fs.statSync(FALL_PDF_SEARCH_INDEX_FILE);
  if (fallPdfSearchIndexCache && fallPdfSearchIndexMtimeMs === stat.mtimeMs) {
    return fallPdfSearchIndexCache;
  }
  fallPdfSearchIndexCache = JSON.parse(fs.readFileSync(FALL_PDF_SEARCH_INDEX_FILE, 'utf8'));
  fallPdfSearchIndexMtimeMs = stat.mtimeMs;
  return fallPdfSearchIndexCache;
}

function listFallPdfDocuments() {
  const data = loadFallPdfSearchIndex();
  return (Array.isArray(data?.documents) ? data.documents : []).map((doc) => ({
    id: String(doc.id || '').trim(),
    title: String(doc.title || '').trim(),
    kind: String(doc.kind || '').trim(),
    url: String(doc.url || '').trim(),
    pageCount: Array.isArray(doc.pages) ? doc.pages.length : 0,
  })).filter((doc) => doc.id && doc.url);
}

function getFallPdfDocument(docId) {
  const data = loadFallPdfSearchIndex();
  const documents = Array.isArray(data?.documents) ? data.documents : [];
  return documents.find((doc) => String(doc.id || '').trim() === docId) || null;
}

function normalizePdfSearchText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function buildFallPdfSearchTerms(query) {
  const normalizedQuery = normalizePdfSearchText(query);
  const terms = [normalizedQuery];
  const blockMatch = normalizedQuery.match(/^(\d{1,3})\s*-\s*(\d{1,3})$/);
  if (blockMatch) {
    terms.push(`${blockMatch[1].padStart(3, '0')}${blockMatch[2].padStart(3, '0')}`);
  }
  if (/^\d{1,6}$/.test(normalizedQuery)) {
    terms.push(normalizedQuery.padStart(6, '0'));
  }
  return Array.from(new Set(
    terms
      .map((term) => term.trim())
      .filter(Boolean)
  ));
}

function buildPdfSearchSnippet(text, query, tokens, matchedTerm = '') {
  const normalized = normalizePdfSearchText(text);
  const lowerText = normalized.toLowerCase();
  const lowerQuery = String(matchedTerm || query || '').toLowerCase();
  let index = lowerText.indexOf(lowerQuery);
  if (index < 0) {
    index = tokens
      .map((token) => lowerText.indexOf(token))
      .filter((position) => position >= 0)
      .sort((a, b) => a - b)[0] ?? 0;
  }
  const start = Math.max(0, index - 90);
  const end = Math.min(normalized.length, index + Math.max(lowerQuery.length, 40) + 120);
  const prefix = start > 0 ? '...' : '';
  const suffix = end < normalized.length ? '...' : '';
  return `${prefix}${normalized.slice(start, end)}${suffix}`;
}

function searchFallPdfDocument(docId, query) {
  const data = loadFallPdfSearchIndex();
  const documents = Array.isArray(data?.documents) ? data.documents : [];
  const doc = documents.find((item) => String(item.id || '').trim() === docId);
  if (!doc) return { doc: null, results: [] };

  const normalizedQuery = normalizePdfSearchText(query);
  const searchTerms = buildFallPdfSearchTerms(normalizedQuery);
  const tokens = normalizedQuery
    .toLowerCase()
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);
  if (!normalizedQuery || !tokens.length) return { doc, results: [], normalizedQuery, searchTerms };

  const lowerSearchTerms = searchTerms.map((term) => term.toLowerCase());
  const pages = Array.isArray(doc.pages) ? doc.pages : [];
  const results = [];
  for (const page of pages) {
    const text = normalizePdfSearchText(page?.text);
    const lowerText = text.toLowerCase();
    const matchedTerm = lowerSearchTerms.find((term) => lowerText.includes(term)) || '';
    const exactMatch = Boolean(matchedTerm);
    const tokenMatch = tokens.every((token) => lowerText.includes(token));
    if (!exactMatch && !tokenMatch) continue;
    results.push({
      page: Number(page?.page || 0) || 0,
      snippet: buildPdfSearchSnippet(text, normalizedQuery, tokens, matchedTerm),
      matchType: exactMatch ? 'exact' : 'all_terms',
      matchedTerm: searchTerms[lowerSearchTerms.indexOf(matchedTerm)] || '',
    });
    if (results.length >= 40) break;
  }
  return { doc, results, normalizedQuery, searchTerms };
}

function getFallbackBookingBoardsUpdatedAt(bookingBoardsData = null) {
  const generatedAt = String(bookingBoardsData?.updatedAt || bookingBoardsData?.generatedAt || '').trim();
  if (generatedAt) return generatedAt;
  try {
    return fs.statSync(BOOKING_BOARDS_DATA_FILE).mtime.toISOString();
  } catch {
    return '';
  }
}

function getBoardUpdatedAt(board, bookingBoardsData = null) {
  const boardUpdatedAt = String(board?.updatedAt || '').trim();
  return boardUpdatedAt || getFallbackBookingBoardsUpdatedAt(bookingBoardsData);
}

function mergeBookingBoardUpdateTimestamps(rebuiltData, previousData, uploadedBoardKeys, updatedAt) {
  const keys = Array.isArray(uploadedBoardKeys) ? uploadedBoardKeys : [uploadedBoardKeys];
  const uploadedBoardIds = new Set(
    keys.flatMap((key) => BOOKING_BOARD_UPLOAD_TARGETS[key]?.boardIds || [])
  );
  const previousFallback = getFallbackBookingBoardsUpdatedAt(previousData);
  const previousById = new Map(
    (Array.isArray(previousData?.boards) ? previousData.boards : [])
      .map((board) => [String(board?.id || '').trim(), board])
      .filter(([id]) => id)
  );
  return {
    ...rebuiltData,
    boards: (Array.isArray(rebuiltData?.boards) ? rebuiltData.boards : []).map((board) => {
      const boardId = String(board?.id || '').trim();
      const previousBoard = previousById.get(boardId);
      return {
        ...board,
        updatedAt: uploadedBoardIds.has(boardId)
          ? updatedAt
          : String(previousBoard?.updatedAt || previousFallback || board?.updatedAt || '').trim(),
      };
    }),
  };
}

function getBookingBoardSummaries() {
  const bookingBoardsData = loadBookingBoardsData();
  const boards = Array.isArray(bookingBoardsData?.boards) ? bookingBoardsData.boards : [];
  return boards
    .filter((board) => {
      const entryCount = Array.isArray(board.entries) ? board.entries.length : 0;
      const sectionCount = Array.isArray(board.sections) ? board.sections.length : 0;
      const counterCount = Array.isArray(board.counters) ? board.counters.length : 0;
      return entryCount > 0 || sectionCount > 0 || counterCount > 0;
    })
    .map((board) => {
      const entries = (Array.isArray(board.entries) ? board.entries : [])
        .map((entry) => buildBookingBoardEntrySummary(entry, board))
        .filter((entry) => board.id !== 'weekend_boards' || !isFullyTakenWeekendEntry(entry));
      const spareSections = (Array.isArray(board.sections) ? board.sections : []).map((section) => buildSpareBoardSectionSummary(section));
      const counters = (Array.isArray(board.counters) ? board.counters : []).map((counter) => buildDaysOffCounterSummary(counter));
      return {
        id: String(board.id || '').trim(),
        title: String(board.title || '').trim(),
        serviceDay: String(board.serviceDay || '').trim(),
        sourcePdf: String(board.sourcePdf || '').trim(),
        updatedAt: getBoardUpdatedAt(board, bookingBoardsData),
        entryCount: counters.length
          ? counters.reduce((sum, counter) => sum + counter.rows.length, 0)
          : spareSections.length
          ? spareSections.reduce((sum, section) => sum + section.garages.length, 0)
          : entries.length,
        takenCount: entries.filter((entry) => entry.taken).length,
        pieceCount: counters.length
          ? counters.reduce((sum, counter) => sum + counter.remaining, 0)
          : spareSections.length
          ? spareSections.reduce((sum, section) => sum + section.openSlotCount, 0)
          : entries.reduce((sum, entry) => sum + entry.pieceCount, 0),
      };
    });
}

function buildBookingBoardResponse(requestedBoardId = '') {
  const summaries = getBookingBoardSummaries();
  const bookingBoardsData = loadBookingBoardsData();
  const updatedAt = getFallbackBookingBoardsUpdatedAt(bookingBoardsData);
  const boards = Array.isArray(bookingBoardsData?.boards) ? bookingBoardsData.boards : [];
  const fallbackBoardId = summaries[0]?.id || '';
  const selectedBoardId = String(requestedBoardId || fallbackBoardId).trim();
  const board = boards.find((item) => String(item.id || '').trim() === selectedBoardId) || null;
  if (!board) {
    return {
      ok: true,
      boards: summaries,
      selectedBoardId: '',
      board: null,
      updatedAt,
      dayOptions: BOOKING_BOARD_DAY_OPTIONS,
    };
  }

  const entries = (Array.isArray(board.entries) ? board.entries : [])
    .map((entry) => buildBookingBoardEntrySummary(entry, board))
    .filter((entry) => board.id !== 'weekend_boards' || !isFullyTakenWeekendEntry(entry))
    .filter((entry) => entry.pieceCount > 0 || entry.taken);
  const sections = (Array.isArray(board.sections) ? board.sections : [])
    .map((section) => buildSpareBoardSectionSummary(section));
  const counters = (Array.isArray(board.counters) ? board.counters : [])
    .map((counter) => buildDaysOffCounterSummary(counter));
  const spareSummary = (Array.isArray(board.spareSummary) ? board.spareSummary : [])
    .map((row) => buildDailySpareSummaryRow(row));

  return {
    ok: true,
    boards: summaries,
    selectedBoardId,
    board: {
      id: String(board.id || '').trim(),
      title: String(board.title || '').trim(),
      serviceDay: String(board.serviceDay || '').trim(),
      sourcePdf: String(board.sourcePdf || '').trim(),
      updatedAt: getBoardUpdatedAt(board, bookingBoardsData),
      entries,
      sections,
      counters,
      spareSummary,
    },
    updatedAt,
    dayOptions: BOOKING_BOARD_DAY_OPTIONS,
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
  try {
    return await getOfficialGtfsBlocks();
  } catch (err) {
    throw new Error(`Failed to read official OC Transpo GTFS block list: ${err.message}`);
  }
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

function getPublicLocationStatusForBlock(block) {
  const paddle = buildPaddleResponse(block);
  const trips = Array.isArray(paddle?.trips) ? paddle.trips : [];
  const finalTrip = trips.length ? trips[trips.length - 1] : null;
  const finalEndSeconds = Number(finalTrip?.timelineEndSeconds);
  const compareSeconds = paddle?.carryover ? getOttawaNowSeconds() + 24 * 3600 : getOttawaNowSeconds();

  return {
    paddle,
    afterFinalTrip: Number.isFinite(finalEndSeconds) && compareSeconds > finalEndSeconds,
  };
}

function hasPublicGtfsTripContext(position = null) {
  return Boolean(String(position?.tripId || position?.trip_id || '').trim());
}

function filterPublicLiveResult(payload = null) {
  if (!payload?.block) return payload;
  const status = getPublicLocationStatusForBlock(payload.block);
  if (!status.afterFinalTrip) return payload;

  return {
    ...payload,
    buses: [],
    gtfsMatch: null,
  };
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
      paddleFetchMs: 0,
      gtfsMs: 0,
    };
    let paddleTrips = [];

    try {
      const startedAt = Date.now();
      paddleTrips = await fetchPaddleTripsForBlock(block);
      timings.paddleFetchMs = Date.now() - startedAt;
    } catch (_) {
      paddleTrips = [];
    }

    if (paddleTrips.length > 0) {
      const startedAt = Date.now();
      const gtfsResult = await fetchGtfsBlockFallback(block, paddleTrips);
      timings.gtfsMs = Date.now() - startedAt;
      if (gtfsResult) {
        return filterPublicLiveResult({ ...gtfsResult, timings });
      }
    }

    return { block, buses: [], liveSource: 'gtfs-rt', timings };
  }).finally(() => {
    pendingByBlock.delete(block);
  });

  pendingByBlock.set(block, job);
  return job;
}

async function fetchRetainedLiveResult(block) {
  const mappings = await getStoredLiveBusPaddleMappingsForBlock(block);
  if (!mappings.length) return null;
  const publicLocationStatus = getPublicLocationStatusForBlock(block);
  const scheduledBreak = Boolean(publicLocationStatus?.paddle?.activeBreak);

  const buses = [];
  for (const mapping of mappings) {
    let officialPosition = null;
    try {
      const lookup = await lookupBusPositionWithGtfsRt(mapping.busNumber);
      officialPosition = lookup?.position || null;
    } catch (_) {
      officialPosition = null;
    }

    const positionTripId = String(officialPosition?.tripId || '').trim();
    if (!canUseRetainedAssignmentForPosition(mapping, officialPosition)) {
      continue;
    }

    const bus = officialPosition
      ? buildGtfsLocationForBus(mapping.busNumber, officialPosition)
      : {
          busNumber: String(mapping.busNumber),
          locationText: 'On break; live location unavailable',
          latitude: null,
          longitude: null,
        };
    if (!positionTripId && !/^On break\b/i.test(bus.locationText)) {
      bus.locationText = `On break — ${bus.locationText}`;
    }
    buses.push(maskLocationForScheduledBreak({
      ...bus,
      assignmentStatus: positionTripId ? 'confirmed' : 'break',
      assignmentVerifiedAt: mapping.verifiedAt,
    }, scheduledBreak));
  }

  if (!buses.length) return null;
  return {
    block,
    buses,
    liveSource: 'gtfs-rt-retained-assignment',
    retainedAssignment: true,
    scheduledBreak,
  };
}

async function fetchLiveResultWithFallback(block) {
  const startedAt = Date.now();
  let payload = filterPublicLiveResult(await withTimeout(fetchLiveResult(block), RUN_TIMEOUT_MS));
  if (!Array.isArray(payload?.buses) || payload.buses.length === 0) {
    const retained = await fetchRetainedLiveResult(block).catch(() => null);
    if (retained) {
      payload = {
        ...payload,
        ...retained,
        timings: payload?.timings || {},
      };
    }
  }
  const scheduledBreak = Boolean(getPublicLocationStatusForBlock(block)?.paddle?.activeBreak);
  if (scheduledBreak && Array.isArray(payload?.buses) && payload.buses.length) {
    payload = {
      ...payload,
      buses: payload.buses.map((bus) => maskLocationForScheduledBreak(bus, true)),
      liveSource: 'gtfs-rt-retained-assignment',
      retainedAssignment: true,
      scheduledBreak: true,
    };
  }
  return {
    ...payload,
    timings: {
      ...(payload?.timings || {}),
      totalLookupMs: Date.now() - startedAt,
    },
  };
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
  const match = text.match(/\b(\d{1,3}\s*-\s*\d{1,3})\b/);
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
  const blockMatch = text.match(/\b(\d{1,3}\s*-\s*\d{1,3})\b/);
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
  if (source === 'none') return 'No live source';
  if (source === 'direct') return 'Direct lookup';
  return source ? source : 'Unknown';
}

function formatTimingLine(timings = {}) {
  const parts = [];
  const totalLookupMs = Number(timings?.totalLookupMs || 0);
  const gtfsMs = Number(timings?.gtfsMs || 0);
  if (totalLookupMs > 0) parts.push(`total ${totalLookupMs}ms`);
  if (gtfsMs > 0) parts.push(`GTFS ${gtfsMs}ms`);
  return parts.length ? `Timing: ${parts.join(' | ')}` : '';
}

function formatChatReply(payload) {
  const buses = Array.isArray(payload?.buses) ? payload.buses : [];
  if (!buses.length) {
    return `Block ${payload?.block || ''}: live bus information isn't available right now.`.trim();
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
    return `Bus ${payload?.busNumber || ''}: live bus information isn't available right now.`.trim();
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
    const storedMapping = await withTimeout(
      getStoredLiveBusPaddleMapping(busNumber),
      1000
    ).catch(() => null);
    let gtfsMatched = null;

    if (isGtfsRtConfigured()) {
      const gtfsStartedAt = Date.now();
      try {
        let cachedBlock = storedMapping?.block || null;
        const gtfsPayload = await lookupBusPositionWithGtfsRt(busNumber);
        const hasCurrentTrip = hasPublicGtfsTripContext(gtfsPayload?.position);
        if (gtfsPayload?.position && (hasCurrentTrip || cachedBlock)) {
          if (hasCurrentTrip && gtfsPayload.position.blockId) {
            cachedBlock = await resolveCanonicalBlock(gtfsPayload.position.blockId).catch(() => null) || normalizeBlock(gtfsPayload.position.blockId);
          } else if (hasCurrentTrip) {
            cachedBlock = null;
          }
          const publicLocationStatus = cachedBlock ? getPublicLocationStatusForBlock(cachedBlock) : null;
          if (!publicLocationStatus?.afterFinalTrip) {
            const scheduledBreak = Boolean(publicLocationStatus?.paddle?.activeBreak);
            let gtfsBus = {
              ...buildGtfsLocationForBus(busNumber, gtfsPayload.position, gtfsMatched?.matched || null),
              assignmentStatus: hasCurrentTrip ? 'confirmed' : 'break',
              assignmentVerifiedAt: hasCurrentTrip ? null : storedMapping?.verifiedAt || null,
            };
            if (!hasCurrentTrip && !/^On break\b/i.test(gtfsBus.locationText)) {
              gtfsBus.locationText = `On break — ${gtfsBus.locationText}`;
            }
            gtfsBus = maskLocationForScheduledBreak(gtfsBus, scheduledBreak);
            payload = {
              busNumber: String(busNumber),
              block: cachedBlock || null,
              buses: [gtfsBus],
              gtfsPosition: gtfsPayload.position,
              gtfsMatched: gtfsMatched?.matched || null,
              parked: !cachedBlock && locationSuggestsParked(gtfsBus?.locationText),
              liveSource: hasCurrentTrip && !scheduledBreak ? 'gtfs-rt' : 'gtfs-rt-retained-assignment',
              retainedAssignment: !hasCurrentTrip || scheduledBreak,
              scheduledBreak,
              timings: {
                gtfsMs: Date.now() - gtfsStartedAt,
              },
            };
          }
        }
      } catch (_) {
        // Report no GTFS-RT result below.
      }
    }

    if (!payload) {
      payload = {
        busNumber: String(busNumber),
        block: storedMapping?.block || null,
        buses: [],
        parked: false,
        liveSource: 'gtfs-rt',
        timings: {
          gtfsMs: Date.now() - startedAt,
        },
      };
    }

    payload.timings = {
      ...(payload.timings || {}),
      totalLookupMs: Date.now() - startedAt,
    };
    const paddle = payload.block ? buildPaddleResponse(payload.block) : null;
    const publicLocationStatus = payload.block ? getPublicLocationStatusForBlock(payload.block) : null;
    const currentTrip = buildCurrentTripSummary(
      publicLocationStatus?.afterFinalTrip || payload.retainedAssignment
        ? null
        : (
          paddle?.activeTrip ||
          payload?.gtfsMatched?.paddleTrip ||
          (payload.liveSource === 'gtfs-rt' ? buildCurrentTripSummaryFromGtfsPosition(payload.gtfsPosition || null) : null) ||
          storedMapping
        )
    );
    const paddleOptions = payload.block ? getPaddleOptionsForBlock(payload.block) : [];

    if (payload.block && !payload.retainedAssignment && Array.isArray(payload.buses) && payload.buses.length) {
      for (const bus of payload.buses) {
        rememberLiveBusPaddleMapping(bus.busNumber, {
          block: payload.block,
          serviceDay: paddle?.serviceDay || storedMapping?.serviceDay || '',
          paddleId: paddle?.paddleId || storedMapping?.paddleId || '',
          route: currentTrip?.route || storedMapping?.route || '',
          headsign: currentTrip?.headsign || storedMapping?.headsign || '',
          tripNumber: currentTrip?.tripNumber || storedMapping?.tripNumber || '',
          startTime: currentTrip?.startTime || storedMapping?.startTime || '',
          endTime: currentTrip?.endTime || storedMapping?.endTime || '',
        });
      }
    }

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
    const publicLocationStatus = getPublicLocationStatusForBlock(block);
    const currentTrip = buildCurrentTripSummary(
      publicLocationStatus.afterFinalTrip || payload?.retainedAssignment
        ? null
        : (paddle?.activeTrip || payload?.gtfsMatch?.paddleTrip)
    );
    const paddleOptions = getPaddleOptionsForBlock(block);
    if (!payload?.retainedAssignment && Array.isArray(payload?.buses) && payload.buses.length) {
      for (const bus of payload.buses) {
        rememberLiveBusPaddleMapping(bus.busNumber, {
          block,
          serviceDay: paddle?.serviceDay || '',
          paddleId: paddle?.paddleId || '',
          route: currentTrip?.route || '',
          headsign: currentTrip?.headsign || '',
          tripNumber: currentTrip?.tripNumber || '',
          startTime: currentTrip?.startTime || '',
          endTime: currentTrip?.endTime || '',
        });
      }
    }
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

async function handleSummerBooking(req, res) {
  const rawBlock = parseBlockFromReq(req);
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const requestedDay = normalizeServiceDay(req.query.day || parseRequestedShuttleDay(parseMessageText(req)));
    const canonicalBlock = await resolveCanonicalBlock(rawBlock).catch(() => null);
    const block = canonicalBlock || rawBlock;
    const paddleOptions = getPinnedVariantPaddleOptionsForBlock(block, SUMMER_PADDLE_VARIANT_ID);
    const selectedDay = requestedDay || paddleOptions[0]?.serviceDay || '';
    const paddle = buildPinnedVariantPaddleResponse(block, SUMMER_PADDLE_VARIANT_ID, selectedDay);
    if (!paddle) {
      res.status(404).json({
        ok: false,
        error: `Summer paddle not found for ${rawBlock}.`,
      });
      return;
    }

    res.json({
      ok: true,
      mode: 'summer-booking',
      block: paddle.block,
      reply: formatSummerBookingReply(paddle),
      paddleOptions,
      paddle,
      generatedAt: new Date().toISOString(),
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

async function handleFallBooking(req, res) {
  const rawBlock = parseBlockFromReq(req);
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const requestedDay = normalizeServiceDay(req.query.day || parseRequestedShuttleDay(parseMessageText(req)));
    const canonicalBlock = await resolveCanonicalBlock(rawBlock).catch(() => null);
    const block = canonicalBlock || rawBlock;
    const paddleOptions = getPinnedVariantPaddleOptionsForBlock(block, FALL_PADDLE_VARIANT_ID);
    const selectedDay = requestedDay || paddleOptions[0]?.serviceDay || '';
    const paddle = buildPinnedVariantPaddleResponse(block, FALL_PADDLE_VARIANT_ID, selectedDay);
    if (!paddle) {
      res.status(404).json({
        ok: false,
        error: `Fall booking paddle not found for ${rawBlock}.`,
      });
      return;
    }

    res.json({
      ok: true,
      mode: 'fall-booking',
      block: paddle.block,
      reply: formatFallBookingReply(paddle),
      paddleOptions,
      paddle,
      generatedAt: new Date().toISOString(),
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

async function handleCanadaDayPaddles(req, res) {
  const rawBlock = parseBlockFromReq(req);
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const canonicalBlock = await resolveCanonicalBlock(rawBlock).catch(() => null);
    const block = canonicalBlock || rawBlock;
    const paddle = buildPinnedVariantPaddleResponse(block, SUMMER_PADDLE_VARIANT_ID, 'canada_day');
    if (!paddle) {
      res.status(404).json({
        ok: false,
        error: `Canada Day paddle not found for ${rawBlock}.`,
      });
      return;
    }

    res.json({
      ok: true,
      mode: 'canada-day-paddles',
      block: paddle.block,
      reply: formatCanadaDayPaddleReply(paddle),
      paddleOptions: [{
        serviceDay: 'canada_day',
        sourceId: paddle.sourceId || null,
        sourceLabel: paddle.sourceLabel || null,
        effective: paddle.effective || null,
        variantId: paddle.variantId || SUMMER_PADDLE_VARIANT_ID,
        variantLabel: paddle.variantLabel || null,
        displayVariantLabel: paddle.displayVariantLabel || null,
        buttonLabel: 'Canada Day',
      }],
      paddle,
      generatedAt: new Date().toISOString(),
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

async function handleCivicHolidayPaddles(req, res) {
  const rawBlock = parseBlockFromReq(req);
  if (!validateBlockOrSend(rawBlock, res)) return;

  try {
    const canonicalBlock = await resolveCanonicalBlock(rawBlock).catch(() => null);
    const block = canonicalBlock || rawBlock;
    const paddle = buildPinnedVariantPaddleResponse(block, SUMMER_PADDLE_VARIANT_ID, 'civic_holiday');
    if (!paddle) {
      res.status(404).json({
        ok: false,
        error: `Civic Holiday paddle not found for ${rawBlock}.`,
      });
      return;
    }

    res.json({
      ok: true,
      mode: 'civic-holiday-paddles',
      block: paddle.block,
      reply: formatCivicHolidayPaddleReply(paddle),
      paddleOptions: [{
        serviceDay: 'civic_holiday',
        sourceId: paddle.sourceId || null,
        sourceLabel: paddle.sourceLabel || null,
        effective: paddle.effective || null,
        variantId: paddle.variantId || SUMMER_PADDLE_VARIANT_ID,
        variantLabel: paddle.variantLabel || null,
        displayVariantLabel: paddle.displayVariantLabel || null,
        buttonLabel: 'Civic Holiday',
      }],
      paddle,
      generatedAt: new Date().toISOString(),
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

async function handleBookingBoards(req, res) {
  try {
    const requestedBoardId = String(req.query.board || '').trim();
    res.json(buildBookingBoardResponse(requestedBoardId));
  } catch (err) {
    res.status(500).json({
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

async function handleShuttlesCatalog(req, res) {
  try {
    res.json(buildShuttleCatalogResponse(req.query.day));
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: String(err.message || 'Unexpected error').slice(0, 500),
    });
  }
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

function isAdminBookingBoardRequest(req) {
  if (!BOOKING_BOARD_ADMIN_TOKEN) return false;
  const providedToken = String(req.get('x-admin-token') || req.query.token || '').trim();
  return crypto.timingSafeEqual(
    Buffer.from(providedToken.padEnd(BOOKING_BOARD_ADMIN_TOKEN.length, '\0').slice(0, BOOKING_BOARD_ADMIN_TOKEN.length)),
    Buffer.from(BOOKING_BOARD_ADMIN_TOKEN)
  ) && providedToken.length === BOOKING_BOARD_ADMIN_TOKEN.length;
}

function normalizeLiveLookupFeedback(body = {}) {
  const issueType = String(body.issueType || '').trim();
  const lookupType = String(body.lookupType || '').trim().toLowerCase();
  const lookupValue = String(body.lookupValue || '').trim().slice(0, 80);
  const reportedBusNumber = String(body.reportedBusNumber || '').trim();
  const correctBusNumber = String(body.correctBusNumber || '').trim();
  const comment = String(body.comment || '').trim().slice(0, 2000);
  if (issueType && !['incorrect_bus_number', 'wrong_bus_location', 'wrong_paddle_information'].includes(issueType)) {
    return { error: 'The selected feedback type is invalid.' };
  }
  if (!['bus', 'block'].includes(lookupType) || !lookupValue) return { error: 'The lookup details are missing.' };
  if (!/^\d{4}$/.test(reportedBusNumber)) return { error: 'The displayed bus number is missing or invalid.' };
  if (correctBusNumber && !/^\d{4}$/.test(correctBusNumber)) return { error: 'The correct bus number must contain four digits.' };
  return {
    value: {
      issue_type: issueType || null,
      lookup_type: lookupType,
      lookup_value: lookupValue,
      block: String(body.block || '').trim().slice(0, 40) || null,
      reported_bus_number: reportedBusNumber,
      correct_bus_number: correctBusNumber || null,
      comment: comment || null,
      live_source: String(body.liveSource || '').trim().slice(0, 120) || null,
      location_text: String(body.locationText || '').trim().slice(0, 500) || null,
      lookup_generated_at: body.lookupGeneratedAt && !Number.isNaN(Date.parse(body.lookupGeneratedAt))
        ? new Date(body.lookupGeneratedAt).toISOString()
        : null,
    },
  };
}

async function handleLiveLookupFeedback(req, res) {
  const normalized = normalizeLiveLookupFeedback(req.body);
  if (normalized.error) {
    res.status(400).json({ ok: false, error: normalized.error });
    return;
  }
  if (!adminSupabase) {
    res.status(501).json({ ok: false, error: 'Feedback storage is not configured yet.' });
    return;
  }
  try {
    normalized.value.realtime_evidence = await captureRealtimeEvidence(normalized.value.reported_bus_number);
  } catch (evidenceError) {
    normalized.value.realtime_evidence = {
      capturedAt: new Date().toISOString(),
      requestedBusNumber: normalized.value.reported_bus_number,
      error: String(evidenceError?.message || 'Feed evidence was unavailable.').slice(0, 500),
    };
  }
  const { data, error } = await adminSupabase
    .from('live_lookup_feedback')
    .insert(normalized.value)
    .select('id, created_at')
    .single();
  if (error) {
    console.error('Live lookup feedback insert failed:', error.message);
    res.status(500).json({ ok: false, error: 'The feedback could not be saved. Please try again.' });
    return;
  }
  res.status(201).json({ ok: true, id: data.id, createdAt: data.created_at });
}

async function handleLiveLookupFeedbackAdmin(req, res) {
  if (!adminSupabase) {
    res.status(501).json({ ok: false, error: 'Feedback storage is not configured yet.' });
    return;
  }
  const authorization = String(req.get('authorization') || '').trim();
  const accessToken = authorization.toLowerCase().startsWith('bearer ')
    ? authorization.slice(7).trim()
    : '';
  if (!accessToken) {
    res.status(401).json({ ok: false, error: 'Sign in with the authorized admin account.' });
    return;
  }
  const { data: authData, error: authError } = await adminSupabase.auth.getUser(accessToken);
  const signedInEmail = String(authData?.user?.email || '').trim().toLowerCase();
  if (authError || !signedInEmail || signedInEmail !== INCIDENT_FEEDBACK_ADMIN_EMAIL) {
    res.status(403).json({ ok: false, error: 'This account does not have access to the feedback log.' });
    return;
  }
  const requestedLimit = Number(req.query.limit || 100);
  const limit = Math.min(250, Math.max(1, Number.isFinite(requestedLimit) ? requestedLimit : 100));
  const { data, error } = await adminSupabase
    .from('live_lookup_feedback')
    .select('*')
    .order('created_at', { ascending: false })
    .limit(limit);
  if (error) {
    console.error('Live lookup feedback fetch failed:', error.message);
    res.status(500).json({ ok: false, error: 'The feedback log could not be loaded.' });
    return;
  }
  let feedback = data || [];
  const status = String(req.query.status || '').trim().toLowerCase();
  const issueType = String(req.query.issueType || '').trim();
  const lookupType = String(req.query.lookupType || '').trim();
  const search = String(req.query.search || '').trim().toLowerCase();
  const datePreset = String(req.query.datePreset || '').trim().toLowerCase();
  const fromDate = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.fromDate || '')) ? String(req.query.fromDate) : '';
  const toDate = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.toDate || '')) ? String(req.query.toDate) : '';
  if (['open', 'resolved'].includes(status)) feedback = feedback.filter((item) => (item.status || 'open') === status);
  if (issueType) feedback = feedback.filter((item) => item.issue_type === issueType);
  if (['bus', 'block'].includes(lookupType)) feedback = feedback.filter((item) => item.lookup_type === lookupType);
  if (search) {
    feedback = feedback.filter((item) => [item.lookup_value, item.block, item.reported_bus_number, item.correct_bus_number, item.comment, item.location_text]
      .some((value) => String(value || '').toLowerCase().includes(search)));
  }
  const reportOttawaDate = (item) => {
    const value = item.lookup_generated_at || item.created_at;
    const date = value ? new Date(value) : null;
    return date && !Number.isNaN(date.getTime()) ? getOttawaDateString(date) : '';
  };
  if (datePreset === 'today') {
    const today = getOttawaDateString();
    feedback = feedback.filter((item) => reportOttawaDate(item) === today);
  } else if (datePreset === 'yesterday') {
    const yesterday = getOttawaDateString(new Date(Date.now() - 24 * 60 * 60 * 1000));
    feedback = feedback.filter((item) => reportOttawaDate(item) === yesterday);
  } else if (datePreset === 'range') {
    if (fromDate) feedback = feedback.filter((item) => reportOttawaDate(item) >= fromDate);
    if (toDate) feedback = feedback.filter((item) => reportOttawaDate(item) <= toDate);
  }
  res.json({ ok: true, feedback });
}

async function authorizeIncidentFeedbackAdmin(req, res) {
  if (!adminSupabase) {
    res.status(501).json({ ok: false, error: 'Feedback storage is not configured yet.' });
    return false;
  }
  const authorization = String(req.get('authorization') || '').trim();
  const accessToken = authorization.toLowerCase().startsWith('bearer ') ? authorization.slice(7).trim() : '';
  if (!accessToken) {
    res.status(401).json({ ok: false, error: 'Sign in with the authorized admin account.' });
    return false;
  }
  const { data, error } = await adminSupabase.auth.getUser(accessToken);
  if (error || String(data?.user?.email || '').trim().toLowerCase() !== INCIDENT_FEEDBACK_ADMIN_EMAIL) {
    res.status(403).json({ ok: false, error: 'This account does not have access to the feedback log.' });
    return false;
  }
  return true;
}

function isFeedbackId(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || ''));
}

async function handleLiveLookupFeedbackUpdate(req, res) {
  if (!await authorizeIncidentFeedbackAdmin(req, res)) return;
  if (!isFeedbackId(req.params.id)) return res.status(400).json({ ok: false, error: 'Invalid feedback ID.' });
  const updates = {};
  if (Object.prototype.hasOwnProperty.call(req.body, 'issueType')) {
    const value = String(req.body.issueType || '').trim();
    if (value && !['incorrect_bus_number', 'wrong_bus_location', 'wrong_paddle_information'].includes(value)) {
      return res.status(400).json({ ok: false, error: 'Invalid feedback type.' });
    }
    updates.issue_type = value || null;
  }
  if (Object.prototype.hasOwnProperty.call(req.body, 'correctBusNumber')) {
    const value = String(req.body.correctBusNumber || '').trim();
    if (value && !/^\d{4}$/.test(value)) return res.status(400).json({ ok: false, error: 'Correct bus number must contain four digits.' });
    updates.correct_bus_number = value || null;
  }
  if (Object.prototype.hasOwnProperty.call(req.body, 'comment')) updates.comment = String(req.body.comment || '').trim().slice(0, 2000) || null;
  if (Object.prototype.hasOwnProperty.call(req.body, 'status')) {
    const value = String(req.body.status || '').trim().toLowerCase();
    if (!['open', 'resolved'].includes(value)) return res.status(400).json({ ok: false, error: 'Invalid feedback status.' });
    updates.status = value;
    updates.resolved_at = value === 'resolved' ? new Date().toISOString() : null;
  }
  if (!Object.keys(updates).length) return res.status(400).json({ ok: false, error: 'No changes were provided.' });
  updates.updated_at = new Date().toISOString();
  const { data, error } = await adminSupabase.from('live_lookup_feedback').update(updates).eq('id', req.params.id).select('*').single();
  if (error) return res.status(500).json({ ok: false, error: 'The feedback could not be updated.' });
  res.json({ ok: true, feedback: data });
}

async function handleLiveLookupFeedbackDelete(req, res) {
  if (!await authorizeIncidentFeedbackAdmin(req, res)) return;
  if (!isFeedbackId(req.params.id)) return res.status(400).json({ ok: false, error: 'Invalid feedback ID.' });
  const { error } = await adminSupabase.from('live_lookup_feedback').delete().eq('id', req.params.id);
  if (error) return res.status(500).json({ ok: false, error: 'The feedback could not be deleted.' });
  res.json({ ok: true });
}

function normalizeBookingBoardUploadName(name) {
  return String(name || '').toLowerCase().replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function classifyBookingBoardUploadName(name) {
  const normalized = normalizeBookingBoardUploadName(name);
  if (normalized.includes('spare')) return 'spares';
  if (normalized.includes('days off') || normalized.includes('day off') || normalized.includes('counter')) return 'days_off_counter';
  if (normalized.includes('vacation')) return 'vacation_tracker';
  if (normalized.includes('stat') || normalized.includes('canada day') || normalized.includes('august civic')) return 'stat';
  if (normalized.includes('weekend') || normalized.includes('saturday') || normalized.includes('sunday')) return 'weekend';
  if (normalized.includes('daily') || normalized.includes('bords') || normalized.includes('board')) return 'daily';
  return '';
}

function normalizeWhatsAppAddress(value) {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, '');
}

function makeTwilioSignatureUrl(req) {
  if (WHATSAPP_PUBLIC_WEBHOOK_URL) return WHATSAPP_PUBLIC_WEBHOOK_URL;
  const proto = String(req.get('x-forwarded-proto') || req.protocol || 'https').split(',')[0].trim();
  const host = String(req.get('x-forwarded-host') || req.get('host') || '').split(',')[0].trim();
  return `${proto}://${host}${req.originalUrl}`;
}

function validateTwilioSignature(req) {
  if (!TWILIO_AUTH_TOKEN) return true;
  const provided = String(req.get('x-twilio-signature') || '').trim();
  if (!provided) return false;
  const params = req.body && typeof req.body === 'object' ? req.body : {};
  const payload = Object.keys(params)
    .sort()
    .reduce((acc, key) => acc + key + String(params[key] ?? ''), makeTwilioSignatureUrl(req));
  const expected = crypto.createHmac('sha1', TWILIO_AUTH_TOKEN).update(payload).digest('base64');
  return crypto.timingSafeEqual(
    Buffer.from(provided.padEnd(expected.length, '\0').slice(0, expected.length)),
    Buffer.from(expected)
  ) && provided.length === expected.length;
}

function isAuthorizedWhatsAppBookingBoardRequest(req) {
  if (!WHATSAPP_BOOKING_BOARD_TOKEN) return false;
  const token = String(req.query.token || req.get('x-whatsapp-booking-token') || '').trim();
  if (token !== WHATSAPP_BOOKING_BOARD_TOKEN) return false;
  if (!validateTwilioSignature(req)) return false;
  if (!WHATSAPP_ALLOWED_FROM.length) return true;
  const sender = normalizeWhatsAppAddress(req.body?.From);
  return WHATSAPP_ALLOWED_FROM.map(normalizeWhatsAppAddress).includes(sender);
}

function escapeXml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function sendTwilioMessage(res, message) {
  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?><Response><Message>${escapeXml(message)}</Message></Response>`);
}

function parseBoardHintsFromText(text) {
  const normalized = normalizeBookingBoardUploadName(text);
  const matches = [];
  const hintAliases = [
    ['days_off_counter', /\b(days?\s+off|counter)\b/g],
    ['vacation_tracker', /\bvacation\b/g],
    ['spares', /\bspares?\b/g],
    ['stat', /\b(stat|civic|canada\s+day|holiday)\b/g],
    ['weekend', /\b(weekend|saturday|sunday)\b/g],
    ['daily', /\b(daily|weekday|weekdays)\b/g],
  ];
  for (const [key, pattern] of hintAliases) {
    for (const match of normalized.matchAll(pattern)) {
      matches.push({ key, index: match.index || 0 });
    }
  }
  const seen = new Set();
  return matches
    .sort((a, b) => a.index - b.index)
    .map((item) => item.key)
    .filter((key) => {
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

async function downloadTwilioMedia(mediaUrl) {
  const headers = {};
  const mediaAuthUser = TWILIO_API_KEY_SID || TWILIO_ACCOUNT_SID;
  const mediaAuthPass = TWILIO_API_KEY_SECRET || TWILIO_AUTH_TOKEN;
  if (mediaAuthUser && mediaAuthPass) {
    headers.Authorization = `Basic ${Buffer.from(`${mediaAuthUser}:${mediaAuthPass}`).toString('base64')}`;
  }
  const response = await fetch(mediaUrl, { headers });
  const buffer = Buffer.from(await response.arrayBuffer());
  if (!response.ok) {
    throw new Error(`Media download failed with HTTP ${response.status}: ${buffer.toString('utf8').slice(0, 300)}`);
  }
  return {
    buffer,
    contentType: String(response.headers.get('content-type') || '').toLowerCase(),
    disposition: String(response.headers.get('content-disposition') || ''),
  };
}

function filenameFromContentDisposition(disposition) {
  const match = String(disposition || '').match(/filename\*?=(?:UTF-8''|")?([^";]+)/i);
  return match ? decodeURIComponent(match[1].replace(/^"|"$/g, '').trim()) : '';
}

function collectTwilioMediaItems(body) {
  const count = Math.max(0, Number.parseInt(String(body?.NumMedia || '0'), 10) || 0);
  const items = [];
  for (let index = 0; index < count; index += 1) {
    const url = String(body?.[`MediaUrl${index}`] || '').trim();
    if (!url) continue;
    items.push({
      index,
      url,
      contentType: String(body?.[`MediaContentType${index}`] || '').toLowerCase(),
    });
  }
  return items;
}

async function classifyBookingBoardPdfBuffer(pdfBuffer) {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'booking-board-classify-'));
  const tempPdf = path.join(tempRoot, 'incoming.pdf');
  const classifyScript = `
import importlib.util
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
pdf_path = Path(sys.argv[2])
spec = importlib.util.spec_from_file_location("builder", root / "tools" / "build_booking_boards.py")
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)
pages = builder.extract_lines(pdf_path)
first_page = " ".join((pages[0].get("lines", [])[:80] if pages else []))
first_page = " ".join(str(first_page or "").replace("_", " ").replace("-", " ").lower().split())
text = " ".join(" ".join(page.get("lines", [])[:80]) for page in pages[:3])
text = " ".join(str(text or "").replace("_", " ").replace("-", " ").lower().split())
board = ""
if "vacation tracker" in text:
    board = "vacation_tracker"
elif "fall 2026 days off" in text or "day total booked remaining" in first_page:
    board = "days_off_counter"
elif "general booking spare progress report" in first_page or "floating spare" in first_page:
    board = "spares"
elif "daily open work" in first_page:
    board = "daily"
elif "mixed odd work saturday" in first_page or "mixed odd work sunday" in text or "sat1 sat2" in first_page:
    board = "weekend"
elif "labour day" in first_page or "stat work" in text or "canada day" in text or "civic" in text or "holiday" in text:
    board = "stat"
print(json.dumps({"board": board}))
`;
  try {
    fs.writeFileSync(tempPdf, pdfBuffer);
    const pythonBins = [PYTHON_BIN, 'python3', 'python'].filter(Boolean);
    for (const pythonBin of [...new Set(pythonBins)]) {
      try {
        const result = await execFileAsync(pythonBin, ['-c', classifyScript, __dirname, tempPdf], {
          cwd: __dirname,
          timeout: 45000,
          maxBuffer: 1024 * 1024,
          env: {
            ...process.env,
            PYTHONPATH: [PYTHON_VENDOR_DIR, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
          },
        });
        return String(JSON.parse(result.stdout || '{}').board || '').trim();
      } catch (err) {
        if (err.code !== 'ENOENT') throw err;
      }
    }
    return '';
  } finally {
    try {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    } catch {}
  }
}

async function handleWhatsAppBookingBoardUpload(req, res) {
  if (!isAuthorizedWhatsAppBookingBoardRequest(req)) {
    sendTwilioMessage(res, 'Booking board WhatsApp upload is not authorized.');
    return;
  }

  const mediaItems = collectTwilioMediaItems(req.body);
  if (!mediaItems.length) {
    sendTwilioMessage(res, 'Send or forward one or more Fall booking board PDF files. You can add a caption like daily, weekend, spare, stat, days off, or vacation.');
    return;
  }

  try {
    const bodyText = String(req.body?.Body || '').trim();
    const bodyHints = parseBoardHintsFromText(bodyText);
    const uploadedTargets = {};
    const uploaded = [];
    const unmatched = [];

    for (const item of mediaItems) {
      const downloaded = await downloadTwilioMedia(item.url);
      const sourceName = filenameFromContentDisposition(downloaded.disposition) || bodyText || `whatsapp-media-${item.index + 1}.pdf`;
      if (!downloaded.buffer.length || downloaded.buffer.subarray(0, 5).toString('utf8') !== '%PDF-') {
        unmatched.push(`${sourceName} was not a PDF.`);
        continue;
      }
      const inferredBoard = classifyBookingBoardUploadName(sourceName)
        || (bodyHints.length === mediaItems.length ? bodyHints[item.index] : '')
        || (mediaItems.length === 1 && bodyHints.length === 1 ? bodyHints[0] : '')
        || await classifyBookingBoardPdfBuffer(downloaded.buffer);
      const target = BOOKING_BOARD_UPLOAD_TARGETS[inferredBoard];
      if (!target || !BOOKING_BOARD_PRIMARY_UPLOAD_KEYS.includes(inferredBoard)) {
        unmatched.push(`${sourceName} could not be matched. Add a caption like daily, weekend, spare, stat, days off, or vacation.`);
        continue;
      }
      if (uploadedTargets[inferredBoard]) {
        throw new Error(`More than one PDF matched ${target.label}. Send one PDF for that board type.`);
      }
      uploadedTargets[inferredBoard] = downloaded.buffer;
      uploaded.push({
        board: inferredBoard,
        label: target.label,
        sourceName,
        filename: target.filename,
      });
    }

    if (!Object.keys(uploadedTargets).length) {
      sendTwilioMessage(res, `No booking boards were updated. ${unmatched.join(' ')}`.trim());
      return;
    }

    const result = await rebuildUploadedBookingBoards(uploadedTargets);
    const updatedLabels = uploaded.map((item) => item.label).join(', ');
    const unmatchedNote = unmatched.length ? ` Unmatched: ${unmatched.join(' ')}` : '';
    sendTwilioMessage(
      res,
      `Updated ${updatedLabels}. ${result.boardCount} boards rebuilt at ${result.updatedAt}. Local runtime updated; use Vercel webhook for permanent GitHub updates.${unmatchedNote}`
    );
  } catch (err) {
    sendTwilioMessage(res, `Booking board update failed: ${String(err.message || err).slice(0, 900)}`);
  }
}

function normalizeFloatingSpareOverrides(rawOverrides) {
  if (!Array.isArray(rawOverrides)) return [];
  const definitionsById = new Map(FLOATING_SPARE_OVERRIDE_DEFINITIONS.map((definition) => [definition.id, definition]));
  const overrides = [];
  for (const item of rawOverrides) {
    const id = String(item?.id || '').trim();
    const definition = definitionsById.get(id);
    if (!definition) continue;
    const available = Number.parseInt(String(item?.available ?? '').trim(), 10);
    if (!Number.isInteger(available) || available < 0 || available > definition.limit) {
      throw new Error(`${definition.title} available spots must be between 0 and ${definition.limit}.`);
    }
    overrides.push({
      ...definition,
      available,
      booked: Math.max(0, definition.limit - available),
    });
  }
  return overrides;
}

function applyFloatingSpareOverrides(bookingBoardsData, rawOverrides) {
  const overrides = normalizeFloatingSpareOverrides(rawOverrides);
  if (!overrides.length) return { data: bookingBoardsData, applied: [] };

  const boards = Array.isArray(bookingBoardsData?.boards) ? bookingBoardsData.boards : [];
  const data = { ...bookingBoardsData, boards: boards.map((board) => ({ ...board })) };
  const sparesBoard = data.boards.find((board) => board?.id === 'spares');
  if (!sparesBoard) return { data, applied: [] };

  const overrideTitles = new Set(overrides.map((override) => override.title));
  const existingSections = Array.isArray(sparesBoard.sections) ? sparesBoard.sections : [];
  const nonFloatingSections = existingSections.filter((section) => (
    section?.kind !== 'floating' && !overrideTitles.has(String(section?.title || ''))
  ));
  const floatingSections = overrides.map((override) => ({
    id: override.id,
    title: override.title,
    page: 1,
    group: 'daily',
    kind: 'floating',
    garages: [{
      name: 'All locations',
      slots: [{
        onTime: '00:00',
        limit: override.limit,
        booked: override.booked,
        available: override.available,
      }],
    }],
  }));
  sparesBoard.sections = [...nonFloatingSections, ...floatingSections];
  return { data, applied: floatingSections.map((section) => section.title) };
}

function copyBookingBoardSourcesToTemp(tempSourceDir) {
  fs.mkdirSync(tempSourceDir, { recursive: true });
  for (const target of Object.values(BOOKING_BOARD_UPLOAD_TARGETS)) {
    const sourcePath = path.join(BOOKING_BOARDS_SOURCE_DIR, target.filename);
    const tempPath = path.join(tempSourceDir, target.filename);
    if (fs.existsSync(sourcePath) && !fs.existsSync(tempPath)) {
      fs.copyFileSync(sourcePath, tempPath);
    }
  }
}

async function rebuildBookingBoardsWithPython(tempSourceDir, tempOutputFile) {
  const pythonBins = [PYTHON_BIN, 'python3', 'python'].filter(Boolean);
  const candidates = [...new Set(pythonBins)];
  const errors = [];

  for (const pythonBin of candidates) {
    try {
      await execFileAsync(pythonBin, [BOOKING_BOARDS_BUILD_SCRIPT], {
        cwd: __dirname,
        timeout: 180000,
        maxBuffer: 4 * 1024 * 1024,
        env: {
          ...process.env,
          BOOKING_BOARDS_SOURCE_DIR: tempSourceDir,
          BOOKING_BOARDS_OUTPUT_FILE: tempOutputFile,
          PYTHONPATH: [PYTHON_VENDOR_DIR, process.env.PYTHONPATH].filter(Boolean).join(path.delimiter),
        },
      });
      return pythonBin;
    } catch (err) {
      errors.push(`${pythonBin}: ${String(err.stderr || err.message || err).slice(0, 500)}`);
      if (err.code !== 'ENOENT') {
        break;
      }
    }
  }

  const message = errors.length ? errors.join('\n') : 'No Python executable was available.';
  throw new Error(message);
}

async function rebuildUploadedBookingBoards(uploadedTargets, options = {}) {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'booking-boards-'));
  const tempSourceDir = path.join(tempRoot, 'Booking_Boards');
  const tempOutputFile = path.join(tempRoot, 'booking_boards.json');
  const uploadedBoardKeys = [...Object.keys(uploadedTargets)];

  try {
    const previousData = loadBookingBoardsData();
    copyBookingBoardSourcesToTemp(tempSourceDir);
    for (const [boardKey, pdfBuffer] of Object.entries(uploadedTargets)) {
      const target = BOOKING_BOARD_UPLOAD_TARGETS[boardKey];
      fs.writeFileSync(path.join(tempSourceDir, target.filename), pdfBuffer);
    }

    const pythonBin = await rebuildBookingBoardsWithPython(tempSourceDir, tempOutputFile);
    let rebuiltData = JSON.parse(fs.readFileSync(tempOutputFile, 'utf8'));
    const floatingOverrideResult = applyFloatingSpareOverrides(rebuiltData, options.floatingSpareOverrides);
    rebuiltData = floatingOverrideResult.data;
    if (floatingOverrideResult.applied.length && !uploadedBoardKeys.includes('spares')) {
      uploadedBoardKeys.push('spares');
    }
    const updatedAt = new Date().toISOString();
    const mergedData = mergeBookingBoardUpdateTimestamps(rebuiltData, previousData, uploadedBoardKeys, updatedAt);
    bookingBoardsDataCache = mergedData;
    bookingBoardsDataMtimeMs = -1;
    bookingBoardsRuntimeUpdatedAt = updatedAt;
    return {
      boardCount: Array.isArray(mergedData?.boards) ? mergedData.boards.length : 0,
      updatedAt,
      runtime: pythonBin,
      floatingSpareOverrides: floatingOverrideResult.applied,
    };
  } finally {
    try {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    } catch {}
  }
}

function sendBookingBoardAdminUnauthorizedOrDisabled(req, res) {
  if (!BOOKING_BOARD_ADMIN_TOKEN) {
    res.status(501).json({
      ok: false,
      error: 'Booking board uploads are disabled. Set BOOKING_BOARD_ADMIN_TOKEN on the server first.',
    });
    return true;
  }
  if (!isAdminBookingBoardRequest(req)) {
    res.status(401).json({
      ok: false,
      error: 'Invalid admin token.',
    });
    return true;
  }
  return false;
}

async function handleBookingBoardUpload(req, res) {
  if (sendBookingBoardAdminUnauthorizedOrDisabled(req, res)) return;

  const boardKey = String(req.params.board || '').trim().toLowerCase();
  const target = BOOKING_BOARD_UPLOAD_TARGETS[boardKey];
  if (!target) {
    res.status(400).json({
      ok: false,
      error: 'Unknown booking board type. Choose one of the listed admin board options.',
    });
    return;
  }

  const pdfBuffer = Buffer.isBuffer(req.body) ? req.body : null;
  if (!pdfBuffer || pdfBuffer.length < 5 || pdfBuffer.subarray(0, 5).toString('utf8') !== '%PDF-') {
    res.status(400).json({
      ok: false,
      error: `Upload a valid PDF file for ${target.label}. It will be saved as ${target.filename}.`,
    });
    return;
  }

  try {
    const result = await rebuildUploadedBookingBoards({ [boardKey]: pdfBuffer });
    res.json({
      ok: true,
      board: boardKey,
      label: target.label,
      filename: target.filename,
      boardCount: result.boardCount,
      updatedAt: result.updatedAt,
      storage: 'runtime-memory',
      runtime: result.runtime,
      note: 'Site update successful. It can take about a minute to show on the site.',
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: `Failed while rebuilding ${target.label} from ${target.filename}: ${String(err.stderr || err.message || 'Booking board rebuild failed').slice(0, 900)}`,
    });
  }
}

async function handleBookingBoardBatchUpload(req, res) {
  if (sendBookingBoardAdminUnauthorizedOrDisabled(req, res)) return;

  try {
    const files = Array.isArray(req.body?.files) ? req.body.files : [];
    const floatingSpareOverrides = Array.isArray(req.body?.floatingSpareOverrides) ? req.body.floatingSpareOverrides : [];
    if (!files.length && !floatingSpareOverrides.length) {
      res.status(400).json({ ok: false, error: 'Choose at least one PDF file or enter floating spare available spots.' });
      return;
    }

    const uploadedTargets = {};
    const uploaded = [];
    const unmatched = [];
    for (const file of files) {
      const sourceName = String(file?.name || '').trim();
      const boardKey = classifyBookingBoardUploadName(sourceName);
      const target = BOOKING_BOARD_UPLOAD_TARGETS[boardKey];
      if (!target || !BOOKING_BOARD_PRIMARY_UPLOAD_KEYS.includes(boardKey)) {
        unmatched.push(sourceName || 'Unnamed PDF');
        continue;
      }
      if (uploadedTargets[boardKey]) {
        res.status(400).json({ ok: false, error: `More than one file matched ${target.label}. Keep one PDF for that board.` });
        return;
      }
      let rawData = String(file?.data || '').trim();
      if (rawData.toLowerCase().startsWith('data:') && rawData.includes(',')) {
        rawData = rawData.split(',', 2)[1];
      }
      const pdfBuffer = Buffer.from(rawData, 'base64');
      if (!pdfBuffer.length || pdfBuffer.subarray(0, 5).toString('utf8') !== '%PDF-') {
        res.status(400).json({ ok: false, error: `${sourceName || target.label} is not a valid PDF.` });
        return;
      }
      uploadedTargets[boardKey] = pdfBuffer;
      uploaded.push({
        board: boardKey,
        label: target.label,
        sourceName,
        filename: target.filename,
      });
    }

    if (!Object.keys(uploadedTargets).length && !floatingSpareOverrides.length) {
      res.status(400).json({ ok: false, error: 'None of the selected PDFs matched a booking board type.' });
      return;
    }

    const result = await rebuildUploadedBookingBoards(uploadedTargets, { floatingSpareOverrides });
    res.json({
      ok: true,
      uploaded,
      missing: BOOKING_BOARD_PRIMARY_UPLOAD_KEYS
        .filter((key) => !uploadedTargets[key])
        .map((key) => ({ board: key, label: BOOKING_BOARD_UPLOAD_TARGETS[key].label })),
      unmatched,
      boardCount: result.boardCount,
      updatedAt: result.updatedAt,
      storage: 'runtime-memory',
      runtime: result.runtime,
      floatingSpareOverrides: result.floatingSpareOverrides,
      note: 'Site update successful. It can take about a minute to show on the site.',
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: `Failed while rebuilding booking boards: ${String(err.stderr || err.message || 'Booking board rebuild failed').slice(0, 900)}`,
    });
  }
}

function handleFallPdfDocuments(_req, res) {
  try {
    res.json({ ok: true, documents: listFallPdfDocuments() });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: `Failed to load Fall PDF list: ${err.message || 'Unknown error'}`,
    });
  }
}

function handleFallPdfSearch(req, res) {
  try {
    const docId = String(req.query.doc || '').trim();
    const query = String(req.query.q || '').trim();
    if (!docId) {
      res.status(400).json({ ok: false, error: 'Choose a PDF first.' });
      return;
    }
    if (!query) {
      const doc = listFallPdfDocuments().find((item) => item.id === docId) || null;
      res.json({ ok: true, doc, query, results: [] });
      return;
    }
    const { doc, results } = searchFallPdfDocument(docId, query);
    if (!doc) {
      res.status(404).json({ ok: false, error: 'Fall PDF not found.' });
      return;
    }
    res.json({
      ok: true,
      doc: {
        id: String(doc.id || '').trim(),
        title: String(doc.title || '').trim(),
        kind: String(doc.kind || '').trim(),
        url: String(doc.url || '').trim(),
        pageCount: Array.isArray(doc.pages) ? doc.pages.length : 0,
      },
      query,
      results,
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: `Fall PDF search failed: ${err.message || 'Unknown error'}`,
    });
  }
}

function handleFallPdfDownload(req, res) {
  try {
    const docId = String(req.query.doc || '').trim();
    const doc = getFallPdfDocument(docId);
    if (!doc) {
      res.status(404).send('Fall PDF not found.');
      return;
    }
    const relativePath = String(doc.path || '').trim();
    const filePath = path.resolve(__dirname, relativePath);
    if (!filePath.startsWith(path.resolve(__dirname) + path.sep) || !fs.existsSync(filePath)) {
      res.status(404).send('Fall PDF file not found.');
      return;
    }
    const filename = `${String(doc.title || 'Fall PDF').replace(/[^\w .-]+/g, '').trim() || 'Fall PDF'}.pdf`;
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.download(filePath, filename);
  } catch (err) {
    res.status(500).send(`Fall PDF download failed: ${err.message || 'Unknown error'}`);
  }
}

app.get('/api/track', handleLookup);
app.post('/api/chat', handleChat);
app.get('/api/paddle', handlePaddle);
app.get('/api/booking-boards', handleBookingBoards);
app.get('/api/fall-pdf-docs', handleFallPdfDocuments);
app.get('/api/fall-pdf-search', handleFallPdfSearch);
app.get('/api/fall-pdf-download', handleFallPdfDownload);
app.post('/api/admin/booking-boards', express.json({ limit: '120mb' }), handleBookingBoardBatchUpload);
app.post('/api/admin/rebuild-booking-board', express.json({ limit: '120mb' }), handleBookingBoardBatchUpload);
app.post('/api/admin/booking-boards/:board', express.raw({ type: ['application/pdf', 'application/octet-stream'], limit: '30mb' }), handleBookingBoardUpload);
app.post('/api/admin/whatsapp-booking-board', express.urlencoded({ extended: false, limit: '2mb' }), handleWhatsAppBookingBoardUpload);
app.get('/api/summer-booking', handleSummerBooking);
app.post('/api/summer-booking', handleSummerBooking);
app.get('/api/fall-booking', handleFallBooking);
app.post('/api/fall-booking', handleFallBooking);
app.get('/api/canada-day-paddles', handleCanadaDayPaddles);
app.post('/api/canada-day-paddles', handleCanadaDayPaddles);
app.get('/api/civic-holiday-paddles', handleCivicHolidayPaddles);
app.post('/api/civic-holiday-paddles', handleCivicHolidayPaddles);
app.get('/api/shuttle', handleShuttle);
app.get('/api/shuttles', handleShuttlesCatalog);
app.get('/api/gtfs-lookup', handleGtfsLookup);
app.get('/api/gtfs-debug', handleGtfsDebug);
app.post('/api/live-lookup-feedback', handleLiveLookupFeedback);
app.get('/api/admin/live-lookup-feedback', handleLiveLookupFeedbackAdmin);
app.patch('/api/admin/live-lookup-feedback/:id', handleLiveLookupFeedbackUpdate);
app.delete('/api/admin/live-lookup-feedback/:id', handleLiveLookupFeedbackDelete);
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
app.get('/api/today-board', (_req, res) => {
  res.status(404).json({
    ok: false,
    error: 'Today Board is temporarily unavailable.',
  });
});
app.get('/vendor/supabase.js', (_req, res) => {
  res.sendFile(path.join(__dirname, 'node_modules', '@supabase', 'supabase-js', 'dist', 'umd', 'supabase.js'));
});
function sendHtmlNoCache(res, filePath) {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  res.sendFile(filePath);
}

app.get('/summer-booking', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'summer-booking.html'));
});
app.get('/summer-paddles', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'summer-booking.html'));
});
app.get('/canada-day-paddles', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'summer-booking.html'));
});
app.get('/civic-holiday-paddles', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'summer-booking.html'));
});
app.get('/fall-paddles', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'summer-booking.html'));
});
app.get('/booking-boards', (_req, res) => {
  res.status(404).type('text/plain').send('Booking boards are offline.');
});
app.get('/support', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'support.html'));
});
app.get('/booking-board-admin', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'booking-board-admin.html'));
});
app.get('/incident-feedback-admin', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'incident-feedback-admin.html'));
});
app.get('/shuttles', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'shuttles.html'));
});
app.get('/today-board', (_req, res) => {
  sendHtmlNoCache(res.status(404), path.join(__dirname, 'public', 'index.html'));
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
    gtfsStatic: getStaticCacheStatus(),
    gtfsWarmup: getGtfsWarmupStatus(),
  });
});

app.get('*', (_req, res) => {
  sendHtmlNoCache(res, path.join(__dirname, 'public', 'index.html'));
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
