'use strict';

const fs = require('fs');
const http = require('http');
const https = require('https');
const path = require('path');
const readline = require('readline');
const zlib = require('zlib');
const protobuf = require('protobufjs');
const unzipper = require('unzipper');
const { parse } = require('csv-parse/sync');

const STATIC_REVALIDATE_MS = Math.max(60 * 1000, Number(process.env.GTFS_STATIC_REVALIDATE_MS || 5 * 60 * 1000));
const STATIC_RETRY_MS = Math.max(30 * 1000, Number(process.env.GTFS_STATIC_RETRY_MS || 60 * 1000));
const REALTIME_TTL_MS = 30 * 1000;
const REALTIME_NETWORK_TIMEOUT_MS = Math.max(1000, Number(process.env.GTFS_RT_TIMEOUT_MS || 8000));
const DEFAULT_HEADER_NAME = 'Ocp-Apim-Subscription-Key';
const DEFAULT_STATIC_INDEX_FILE = path.join(__dirname, '..', 'data', 'gtfs_static_index.json.gz');

const GTFS_RT_PROTO = `
syntax = "proto2";
package transit_realtime;

message FeedMessage {
  required FeedHeader header = 1;
  repeated FeedEntity entity = 2;
}

message FeedHeader {
  required string gtfs_realtime_version = 1;
  optional uint64 timestamp = 3;
}

message FeedEntity {
  required string id = 1;
  optional bool is_deleted = 2 [default = false];
  optional TripUpdate trip_update = 3;
  optional VehiclePosition vehicle = 4;
}

message TripDescriptor {
  optional string trip_id = 1;
  optional string route_id = 5;
}

message VehicleDescriptor {
  optional string id = 1;
  optional string label = 2;
}

message Position {
  required float latitude = 1;
  required float longitude = 2;
  optional float bearing = 3;
}

message TripUpdate {
  required TripDescriptor trip = 1;
  optional VehicleDescriptor vehicle = 3;
  repeated StopTimeUpdate stop_time_update = 2;
  optional uint64 timestamp = 4;
}

message StopTimeEvent {
  optional int32 delay = 1;
  optional int64 time = 2;
}

message StopTimeUpdate {
  optional uint32 stop_sequence = 1;
  optional string stop_id = 4;
  optional StopTimeEvent arrival = 2;
  optional StopTimeEvent departure = 3;
}

message VehiclePosition {
  optional TripDescriptor trip = 1;
  optional VehicleDescriptor vehicle = 8;
  optional Position position = 2;
  optional uint64 timestamp = 5;
  optional uint32 current_stop_sequence = 3;
  optional string stop_id = 7;
}
`;

const feedRoot = protobuf.parse(GTFS_RT_PROTO).root;
const FeedMessage = feedRoot.lookupType('transit_realtime.FeedMessage');

let staticCache = null;
let staticRefreshPromise = null;
let lastMismatchRevalidateAt = 0;
let realtimeCache = null;
let warmupPromise = null;
let warmupStatus = {
  startedAt: null,
  finishedAt: null,
  durationMs: null,
  ok: false,
  error: '',
};

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      return String(value).trim();
    }
  }
  return '';
}

function getConfig() {
  return {
    staticUrl: String(process.env.OCTRANSPO_GTFS_STATIC_URL || '').trim(),
    staticIndexFile: String(process.env.OCTRANSPO_GTFS_STATIC_INDEX_FILE || DEFAULT_STATIC_INDEX_FILE).trim(),
    tripUpdatesUrl: String(process.env.OCTRANSPO_GTFS_RT_TRIP_UPDATES_URL || '').trim(),
    vehiclePositionsUrl: String(process.env.OCTRANSPO_GTFS_RT_VEHICLE_POSITIONS_URL || '').trim(),
    apiKey: String(process.env.OCTRANSPO_GTFS_API_KEY || '').trim(),
    apiKeyHeader: String(process.env.OCTRANSPO_GTFS_API_KEY_HEADER || DEFAULT_HEADER_NAME).trim() || DEFAULT_HEADER_NAME,
  };
}

function isHttpSource(value) {
  return /^https?:\/\//i.test(String(value || '').trim());
}

function isLocalSource(value) {
  const source = String(value || '').trim();
  return Boolean(source) && !isHttpSource(source);
}

function isConfigured() {
  const config = getConfig();
  const hasSources = Boolean(config.staticUrl && config.tripUpdatesUrl && config.vehiclePositionsUrl);
  if (!hasSources) return false;
  const needsApiKey = [config.staticUrl, config.tripUpdatesUrl, config.vehiclePositionsUrl].some(isHttpSource);
  return needsApiKey ? Boolean(config.apiKey) : true;
}

function resolveLocalSource(source) {
  const raw = String(source || '').trim();
  if (raw.startsWith('file://')) {
    return raw.slice('file://'.length);
  }
  return path.isAbsolute(raw) ? raw : path.resolve(process.cwd(), raw);
}

function networkGetBuffer(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const getter = /^http:\/\//i.test(url) ? http : https;
    const req = getter.get(url, { headers }, (res) => {
      if (res.statusCode && res.statusCode >= 400) {
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        res.resume();
        return;
      }
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    });
    req.on('error', reject);
    req.setTimeout(REALTIME_NETWORK_TIMEOUT_MS, () => req.destroy(new Error(`Timeout fetching ${url}`)));
  });
}

function networkGetBufferWithMetadata(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const getter = /^http:\/\//i.test(url) ? http : https;
    const req = getter.get(url, { headers }, (res) => {
      if (res.statusCode && res.statusCode >= 400) {
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        res.resume();
        return;
      }
      if (res.statusCode === 304) {
        res.resume();
        resolve({ statusCode: 304, headers: res.headers, buffer: null });
        return;
      }
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({
        statusCode: res.statusCode || 200,
        headers: res.headers,
        buffer: Buffer.concat(chunks),
      }));
    });
    req.on('error', reject);
    req.setTimeout(REALTIME_NETWORK_TIMEOUT_MS, () => req.destroy(new Error(`Timeout fetching ${url}`)));
  });
}

async function getBufferFromSource(source, headers = {}) {
  if (isHttpSource(source)) {
    return networkGetBuffer(source, headers);
  }
  return fs.promises.readFile(resolveLocalSource(source));
}

function parseCsvBuffer(buffer) {
  return parse(buffer, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    relax_quotes: true,
  });
}

function parseCsvLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

function createCacheKey() {
  return '*';
}

function parseServiceTime(value) {
  const match = String(value || '').trim().match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;
  return Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3] || 0);
}

function normalizeHeadsign(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/station/g, 'stn')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizeStopLabel(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\bst[.-]?\b/g, 'st ')
    .replace(/\bstation\b/g, ' ')
    .replace(/\bstn\b/g, ' ')
    .replace(/\bbus stop\b/g, ' ')
    .replace(/\baeroport\b/g, 'airport')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

async function buildStaticIndexFromDirectory(staticSourcePath) {
  const [routesRows, tripsRows, stopsRows] = await Promise.all([
    fs.promises.readFile(path.join(staticSourcePath, 'routes.txt')).then(parseCsvBuffer),
    fs.promises.readFile(path.join(staticSourcePath, 'trips.txt')).then(parseCsvBuffer),
    fs.promises.readFile(path.join(staticSourcePath, 'stops.txt')).then(parseCsvBuffer),
  ]);

  const routeShortNames = new Map();
  for (const row of routesRows) {
    routeShortNames.set(String(row.route_id || '').trim(), String(row.route_short_name || '').trim());
  }

  const stopsById = new Map();
  for (const row of stopsRows) {
    const stopId = String(row.stop_id || '').trim();
    if (!stopId) continue;
    stopsById.set(stopId, {
      stopId,
      name: String(row.stop_name || '').trim(),
      latitude: Number(row.stop_lat),
      longitude: Number(row.stop_lon),
    });
  }

  const tripsById = new Map();
  for (const row of tripsRows) {
    const tripId = String(row.trip_id || '').trim();
    const routeId = String(row.route_id || '').trim();
    const routeShortName = routeShortNames.get(routeId) || '';
    if (!tripId) continue;
    tripsById.set(tripId, {
      tripId,
      routeId,
      routeShortName,
      serviceId: String(row.service_id || '').trim(),
      headsign: String(row.trip_headsign || '').trim(),
      blockId: String(row.block_id || '').trim(),
    });
  }

  const tripStopBounds = new Map();
  const stopTimesPath = path.join(staticSourcePath, 'stop_times.txt');
  const rl = readline.createInterface({
    input: fs.createReadStream(stopTimesPath),
    crlfDelay: Infinity,
  });

  let headers = null;
  let tripIdIndex = -1;
  let arrivalIndex = -1;
  let departureIndex = -1;
  let stopIdIndex = -1;
  let stopSequenceIndex = -1;

  for await (const rawLine of rl) {
    if (!headers) {
      headers = parseCsvLine(rawLine);
      tripIdIndex = headers.indexOf('trip_id');
      arrivalIndex = headers.indexOf('arrival_time');
      departureIndex = headers.indexOf('departure_time');
      stopIdIndex = headers.indexOf('stop_id');
      stopSequenceIndex = headers.indexOf('stop_sequence');
      continue;
    }
    if (!rawLine) continue;
    const cols = parseCsvLine(rawLine);
    const tripId = String(cols[tripIdIndex] || '').trim();
    if (!tripId || !tripsById.has(tripId)) continue;
    const stopSequence = Number(cols[stopSequenceIndex] || 0);
    const arrival = String(cols[arrivalIndex] || '').trim();
    const departure = String(cols[departureIndex] || '').trim();
    const stopId = String(cols[stopIdIndex] || '').trim();

    const current = tripStopBounds.get(tripId) || {
      firstSequence: Number.POSITIVE_INFINITY,
      lastSequence: Number.NEGATIVE_INFINITY,
      firstStopId: '',
      lastStopId: '',
      firstDeparture: '',
      lastArrival: '',
    };

    if (Number.isFinite(stopSequence) && stopSequence < current.firstSequence) {
      current.firstSequence = stopSequence;
      current.firstStopId = stopId;
      current.firstDeparture = departure || arrival;
    }
    if (Number.isFinite(stopSequence) && stopSequence > current.lastSequence) {
      current.lastSequence = stopSequence;
      current.lastStopId = stopId;
      current.lastArrival = arrival || departure;
    }
    tripStopBounds.set(tripId, current);
  }

  const enrichedTrips = new Map();
  for (const [tripId, trip] of tripsById.entries()) {
    const bounds = tripStopBounds.get(tripId);
    if (!bounds) continue;
    enrichedTrips.set(tripId, {
      ...trip,
      firstStopId: bounds.firstStopId,
      lastStopId: bounds.lastStopId,
      firstStopName: stopsById.get(bounds.firstStopId)?.name || '',
      lastStopName: stopsById.get(bounds.lastStopId)?.name || '',
      firstDeparture: bounds.firstDeparture,
      lastArrival: bounds.lastArrival,
    });
  }

  return createIndexedStaticValue(enrichedTrips, stopsById);
}

function addTripToRouteIndex(tripsByRoute, key, trip) {
  const normalizedKey = String(key || '').trim();
  if (!normalizedKey) return;
  if (!tripsByRoute.has(normalizedKey)) {
    tripsByRoute.set(normalizedKey, []);
  }
  tripsByRoute.get(normalizedKey).push(trip);
}

function createIndexedStaticValue(tripsById, stopsById = new Map()) {
  const tripsByRoute = new Map();
  for (const trip of tripsById.values()) {
    addTripToRouteIndex(tripsByRoute, trip.routeShortName, trip);
    addTripToRouteIndex(tripsByRoute, trip.routeId, trip);
    addTripToRouteIndex(tripsByRoute, String(trip.routeId || '').split('-')[0], trip);
  }
  return { tripsById, tripsByRoute, stopsById };
}

function hydratePrebuiltStaticIndex(payload) {
  const stopsById = new Map();
  for (const row of payload?.stops || []) {
    const [stopId, name, latitude, longitude] = row;
    if (!stopId) continue;
    stopsById.set(String(stopId), {
      stopId: String(stopId),
      name: String(name || ''),
      latitude: Number(latitude),
      longitude: Number(longitude),
    });
  }

  const tripsById = new Map();
  for (const row of payload?.trips || []) {
    const [
      tripId,
      routeId,
      routeShortName,
      serviceId,
      headsign,
      blockId,
      firstStopId,
      lastStopId,
      firstStopName,
      lastStopName,
      firstDeparture,
      lastArrival,
    ] = row;
    if (!tripId) continue;
    tripsById.set(String(tripId), {
      tripId: String(tripId),
      routeId: String(routeId || ''),
      routeShortName: String(routeShortName || ''),
      serviceId: String(serviceId || ''),
      headsign: String(headsign || ''),
      blockId: String(blockId || ''),
      firstStopId: String(firstStopId || ''),
      lastStopId: String(lastStopId || ''),
      firstStopName: String(firstStopName || ''),
      lastStopName: String(lastStopName || ''),
      firstDeparture: String(firstDeparture || ''),
      lastArrival: String(lastArrival || ''),
    });
  }

  return createIndexedStaticValue(tripsById, stopsById);
}

function loadPrebuiltStaticIndex(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const raw = fs.readFileSync(filePath);
  const text = filePath.endsWith('.gz') ? zlib.gunzipSync(raw).toString('utf8') : raw.toString('utf8');
  return hydratePrebuiltStaticIndex(JSON.parse(text));
}

function listAvailableBlocks(staticIndex) {
  const blocks = new Set();
  for (const trip of staticIndex?.tripsById?.values?.() || []) {
    const blockId = String(trip?.blockId || '').trim().toUpperCase();
    if (blockId) blocks.add(blockId);
  }
  return [...blocks].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
}

async function getAvailableBlocks() {
  return listAvailableBlocks(await loadStaticIndex());
}

async function loadStaticIndexUncoordinated(routeIds = [], options = {}) {
  const now = Date.now();
  const cacheKey = createCacheKey();
  if (!options.forceRevalidate && staticCache?.key === cacheKey && staticCache.expiresAt > now) {
    return staticCache.value;
  }

  const config = getConfig();
  if (config.staticIndexFile && fs.existsSync(config.staticIndexFile)) {
    const value = loadPrebuiltStaticIndex(config.staticIndexFile);
    staticCache = {
      key: cacheKey,
      value,
      expiresAt: now + STATIC_REVALIDATE_MS,
      checkedAt: new Date(now).toISOString(),
      downloadedAt: new Date(now).toISOString(),
      etag: '',
      lastModified: '',
      lastError: '',
    };
    return value;
  }
  const headers = config.apiKey ? { [config.apiKeyHeader]: config.apiKey } : {};
  const staticSourcePath = isLocalSource(config.staticUrl) ? resolveLocalSource(config.staticUrl) : '';
  const staticSourceStats = staticSourcePath ? await fs.promises.stat(staticSourcePath) : null;
  if (staticSourceStats?.isDirectory()) {
    const value = await buildStaticIndexFromDirectory(staticSourcePath);
    staticCache = {
      key: cacheKey,
      value,
      expiresAt: now + STATIC_REVALIDATE_MS,
      checkedAt: new Date(now).toISOString(),
      downloadedAt: new Date(now).toISOString(),
      etag: '',
      lastModified: '',
      lastError: '',
    };
    return value;
  }

  let staticBuffer;
  let responseHeaders = {};
  if (isHttpSource(config.staticUrl)) {
    const conditionalHeaders = { ...headers };
    if (staticCache?.etag) conditionalHeaders['If-None-Match'] = staticCache.etag;
    if (staticCache?.lastModified) conditionalHeaders['If-Modified-Since'] = staticCache.lastModified;
    const response = await networkGetBufferWithMetadata(config.staticUrl, conditionalHeaders);
    if (response.statusCode === 304 && staticCache?.value) {
      staticCache = {
        ...staticCache,
        expiresAt: now + STATIC_REVALIDATE_MS,
        checkedAt: new Date(now).toISOString(),
        lastError: '',
      };
      return staticCache.value;
    }
    staticBuffer = response.buffer;
    responseHeaders = response.headers || {};
  } else {
    staticBuffer = await getBufferFromSource(config.staticUrl, headers);
  }
  const staticZip = await unzipper.Open.buffer(staticBuffer);
  const readEntryBuffer = async (entryName) => {
    const entry = staticZip.files.find((file) => (
      String(file.path || '').replace(/\\/g, '/').split('/').pop() === entryName
    ));
    if (!entry) {
      throw new Error(`GTFS static feed is missing ${entryName}`);
    }
    return entry.buffer();
  };

  const [routesRows, tripsRows, stopTimesRows, stopsRows] = await Promise.all([
    readEntryBuffer('routes.txt').then(parseCsvBuffer),
    readEntryBuffer('trips.txt').then(parseCsvBuffer),
    readEntryBuffer('stop_times.txt').then(parseCsvBuffer),
    readEntryBuffer('stops.txt').then(parseCsvBuffer),
  ]);

  const routeShortNames = new Map();
  for (const row of routesRows) {
    routeShortNames.set(String(row.route_id || '').trim(), String(row.route_short_name || '').trim());
  }

  const stopsById = new Map();
  for (const row of stopsRows) {
    const stopId = String(row.stop_id || '').trim();
    if (!stopId) continue;
    stopsById.set(stopId, {
      stopId,
      name: String(row.stop_name || '').trim(),
      latitude: Number(row.stop_lat),
      longitude: Number(row.stop_lon),
    });
  }

  const tripsById = new Map();
  for (const row of tripsRows) {
    const tripId = String(row.trip_id || '').trim();
    if (!tripId) continue;
    tripsById.set(tripId, {
      tripId,
      routeId: String(row.route_id || '').trim(),
      routeShortName: routeShortNames.get(String(row.route_id || '').trim()) || '',
      serviceId: String(row.service_id || '').trim(),
      headsign: String(row.trip_headsign || '').trim(),
      blockId: String(row.block_id || '').trim(),
    });
  }

  const tripStopBounds = new Map();
  for (const row of stopTimesRows) {
    const tripId = String(row.trip_id || '').trim();
    if (!tripId || !tripsById.has(tripId)) continue;
    const stopSequence = Number(row.stop_sequence || 0);
    const arrival = String(row.arrival_time || '').trim();
    const departure = String(row.departure_time || '').trim();
    const stopId = String(row.stop_id || '').trim();

    const current = tripStopBounds.get(tripId) || {
      firstSequence: Number.POSITIVE_INFINITY,
      lastSequence: Number.NEGATIVE_INFINITY,
      firstStopId: '',
      lastStopId: '',
      firstDeparture: '',
      lastArrival: '',
    };

    if (Number.isFinite(stopSequence) && stopSequence < current.firstSequence) {
      current.firstSequence = stopSequence;
      current.firstStopId = stopId;
      current.firstDeparture = departure || arrival;
    }
    if (Number.isFinite(stopSequence) && stopSequence > current.lastSequence) {
      current.lastSequence = stopSequence;
      current.lastStopId = stopId;
      current.lastArrival = arrival || departure;
    }
    tripStopBounds.set(tripId, current);
  }

  const enrichedTrips = new Map();
  for (const [tripId, trip] of tripsById.entries()) {
    const bounds = tripStopBounds.get(tripId);
    if (!bounds) continue;
    enrichedTrips.set(tripId, {
      ...trip,
      firstStopId: bounds.firstStopId,
      lastStopId: bounds.lastStopId,
      firstStopName: stopsById.get(bounds.firstStopId)?.name || '',
      lastStopName: stopsById.get(bounds.lastStopId)?.name || '',
      firstDeparture: bounds.firstDeparture,
      lastArrival: bounds.lastArrival,
    });
  }

  const value = createIndexedStaticValue(enrichedTrips, stopsById);
  staticCache = {
    key: cacheKey,
    value,
    expiresAt: now + STATIC_REVALIDATE_MS,
    checkedAt: new Date(now).toISOString(),
    downloadedAt: new Date(now).toISOString(),
    etag: String(responseHeaders.etag || ''),
    lastModified: String(responseHeaders['last-modified'] || ''),
    lastError: '',
  };
  return value;
}

async function loadStaticIndex(routeIds = [], options = {}) {
  const now = Date.now();
  const cacheKey = createCacheKey();
  if (!options.forceRevalidate && staticCache?.key === cacheKey && staticCache.expiresAt > now) {
    return staticCache.value;
  }
  if (staticRefreshPromise) return staticRefreshPromise;

  staticRefreshPromise = loadStaticIndexUncoordinated(routeIds, options)
    .catch((error) => {
      if (!staticCache?.value) throw error;
      staticCache = {
        ...staticCache,
        expiresAt: Date.now() + STATIC_RETRY_MS,
        checkedAt: new Date().toISOString(),
        lastError: String(error?.message || error || 'Static GTFS refresh failed'),
      };
      return staticCache.value;
    })
    .finally(() => {
      staticRefreshPromise = null;
    });
  return staticRefreshPromise;
}

function findNearestStop(position, staticIndex) {
  const latitude = Number(position?.position?.latitude ?? position?.latitude);
  const longitude = Number(position?.position?.longitude ?? position?.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;

  let best = null;
  for (const stop of staticIndex?.stopsById?.values?.() || []) {
    if (!Number.isFinite(stop.latitude) || !Number.isFinite(stop.longitude) || !stop.name) continue;
    const latDiff = latitude - stop.latitude;
    const lonDiff = longitude - stop.longitude;
    const distance = (latDiff * latDiff) + (lonDiff * lonDiff);
    if (!best || distance < best.distance) {
      best = { ...stop, distance };
    }
  }
  return best;
}

function decodeFeed(buffer) {
  const firstByte = buffer.find((byte) => !/\s/.test(String.fromCharCode(byte)));
  if (firstByte === 0x7b || firstByte === 0x5b) {
    const normalizeJsonKeys = (value) => {
      if (Array.isArray(value)) return value.map(normalizeJsonKeys);
      if (!value || typeof value !== 'object') return value;
      return Object.fromEntries(
        Object.entries(value).map(([key, nestedValue]) => [
          key ? `${key[0].toLowerCase()}${key.slice(1)}` : key,
          normalizeJsonKeys(nestedValue),
        ])
      );
    };
    return FeedMessage.fromObject(normalizeJsonKeys(JSON.parse(buffer.toString('utf8'))));
  }
  return FeedMessage.decode(buffer);
}

function buildRealtimeIndexes(tripUpdatesFeed, vehiclePositionsFeed) {
  const updatesByTripId = new Map();
  const vehicleByTripId = new Map();
  const positionsByVehicleId = new Map();

  for (const entity of tripUpdatesFeed.entity || []) {
    const tripUpdate = entity.trip_update;
    const normalizedTripUpdate = tripUpdate || entity.tripUpdate;
    const tripDescriptor = normalizedTripUpdate?.trip || null;
    if (!normalizedTripUpdate || !tripDescriptor) continue;
    const tripId = firstNonEmpty(tripDescriptor.trip_id, tripDescriptor.tripId);
    if (!tripId) continue;
    const vehicleDescriptor = normalizedTripUpdate.vehicle || null;
    const vehicleId = firstNonEmpty(vehicleDescriptor?.id, vehicleDescriptor?.label);
    updatesByTripId.set(tripId, normalizedTripUpdate);
    if (vehicleId) {
      vehicleByTripId.set(tripId, vehicleId);
    }
  }

  for (const entity of vehiclePositionsFeed.entity || []) {
    const vehicle = entity.vehicle || entity.vehiclePosition;
    if (!vehicle) continue;
    const vehicleDescriptor = vehicle.vehicle || null;
    const tripDescriptor = vehicle.trip || null;
    const vehicleId = firstNonEmpty(vehicleDescriptor?.id, vehicleDescriptor?.label);
    const tripId = firstNonEmpty(tripDescriptor?.trip_id, tripDescriptor?.tripId);
    if (vehicleId) {
      positionsByVehicleId.set(vehicleId, vehicle);
    }
  }

  return { updatesByTripId, vehicleByTripId, positionsByVehicleId };
}

async function loadRealtimeSnapshot() {
  const now = Date.now();
  if (realtimeCache && realtimeCache.expiresAt > now) {
    return realtimeCache.value;
  }

  try {
    const config = getConfig();
    const headers = config.apiKey ? { [config.apiKeyHeader]: config.apiKey } : {};
    const [tripUpdatesBuf, vehiclePositionsBuf] = await Promise.all([
      getBufferFromSource(config.tripUpdatesUrl, headers),
      getBufferFromSource(config.vehiclePositionsUrl, headers),
    ]);

    const tripUpdatesFeed = decodeFeed(tripUpdatesBuf);
    const vehiclePositionsFeed = decodeFeed(vehiclePositionsBuf);

    const value = buildRealtimeIndexes(tripUpdatesFeed, vehiclePositionsFeed);
    realtimeCache = { value, expiresAt: now + REALTIME_TTL_MS };
    return value;
  } catch (error) {
    if (realtimeCache?.value) {
      realtimeCache.expiresAt = now + 5000;
      return realtimeCache.value;
    }
    throw error;
  }
}

function hasRealtimeTripIdMismatch(staticIndex, realtime) {
  if (!staticIndex?.tripsById || !realtime) return false;
  for (const position of realtime.positionsByVehicleId?.values?.() || []) {
    const tripId = firstNonEmpty(position?.trip?.trip_id, position?.trip?.tripId);
    if (tripId && !staticIndex.tripsById.has(tripId)) return true;
  }
  for (const tripId of realtime.updatesByTripId?.keys?.() || []) {
    if (tripId && !staticIndex.tripsById.has(tripId)) return true;
  }
  return false;
}

async function refreshStaticForRealtimeMismatch(staticIndex, realtime) {
  if (!hasRealtimeTripIdMismatch(staticIndex, realtime)) return staticIndex;
  const now = Date.now();
  if (now - lastMismatchRevalidateAt < STATIC_REVALIDATE_MS) return staticIndex;
  lastMismatchRevalidateAt = now;
  return loadStaticIndex([], { forceRevalidate: true });
}

function getStaticCacheStatus() {
  return {
    loaded: Boolean(staticCache?.value),
    refreshing: Boolean(staticRefreshPromise),
    checkedAt: staticCache?.checkedAt || null,
    downloadedAt: staticCache?.downloadedAt || null,
    expiresAt: staticCache?.expiresAt ? new Date(staticCache.expiresAt).toISOString() : null,
    etag: staticCache?.etag || '',
    lastModified: staticCache?.lastModified || '',
    lastError: staticCache?.lastError || '',
    tripCount: staticCache?.value?.tripsById?.size || 0,
  };
}

function toEvidenceJson(value) {
  if (!value) return null;
  return JSON.parse(JSON.stringify(value, (_key, nested) => (
    typeof nested === 'bigint' ? nested.toString() : nested
  )));
}

async function captureRealtimeEvidence(busNumber) {
  const requestedBus = String(busNumber || '').trim();
  const realtime = await loadRealtimeSnapshot();
  let vehicleId = requestedBus;
  let vehiclePosition = realtime.positionsByVehicleId.get(vehicleId) || null;
  let tripId = firstNonEmpty(vehiclePosition?.trip?.trip_id, vehiclePosition?.trip?.tripId);
  if (!tripId) {
    const tripMatch = [...realtime.vehicleByTripId.entries()].find(([, id]) => String(id || '').trim() === requestedBus);
    tripId = tripMatch?.[0] || '';
  }
  const tripUpdate = tripId ? (realtime.updatesByTripId.get(tripId) || null) : null;
  const plainTripUpdate = toEvidenceJson(tripUpdate);
  const stopUpdates = plainTripUpdate?.stop_time_update || plainTripUpdate?.stopTimeUpdate || [];
  if (plainTripUpdate && Array.isArray(stopUpdates) && stopUpdates.length > 20) {
    if (plainTripUpdate.stop_time_update) plainTripUpdate.stop_time_update = stopUpdates.slice(0, 20);
    if (plainTripUpdate.stopTimeUpdate) plainTripUpdate.stopTimeUpdate = stopUpdates.slice(0, 20);
  }
  return {
    capturedAt: new Date().toISOString(),
    requestedBusNumber: requestedBus,
    matchedVehicleId: vehiclePosition ? vehicleId : null,
    matchedTripId: tripId || null,
    vehiclePosition: toEvidenceJson(vehiclePosition),
    tripUpdate: plainTripUpdate,
  };
}

function normalizeBlockId(value) {
  const text = String(value || '').trim().toLowerCase();
  const numeric = text.match(/^0*(\d{1,3})\s*-\s*0*(\d{1,3})$/);
  if (numeric) {
    return `${Number(numeric[1])}-${Number(numeric[2])}`;
  }
  return text;
}

function scoreGtfsTripMatch(paddleTrip, gtfsTrip, context = {}) {
  let score = 0;
  const paddleRoute = String(paddleTrip.routeId || '').trim();
  const gtfsRouteId = String(gtfsTrip.routeId || '').trim();
  const gtfsRouteShortName = String(gtfsTrip.routeShortName || '').trim();
  const gtfsRouteComparable = gtfsRouteShortName || gtfsRouteId;
  if (paddleRoute && gtfsRouteComparable && paddleRoute === gtfsRouteComparable) score += 10;
  else if (paddleRoute && gtfsRouteId && gtfsRouteId.split('-')[0] === paddleRoute) score += 7;

  const paddleStart = String(paddleTrip.scheduledStartTime || '').slice(0, 5);
  const paddleEnd = String(paddleTrip.scheduledEndTime || '').slice(0, 5);
  const gtfsStart = String(gtfsTrip.firstDeparture || '').slice(0, 5);
  const gtfsEnd = String(gtfsTrip.lastArrival || '').slice(0, 5);

  if (paddleStart && gtfsStart && paddleStart === gtfsStart) score += 8;
  if (paddleEnd && gtfsEnd && paddleEnd === gtfsEnd) score += 8;

  const paddleStartSeconds = parseServiceTime(paddleTrip.scheduledStartTime);
  const paddleEndSeconds = parseServiceTime(paddleTrip.scheduledEndTime);
  const gtfsStartSeconds = parseServiceTime(gtfsTrip.firstDeparture);
  const gtfsEndSeconds = parseServiceTime(gtfsTrip.lastArrival);

  if (paddleStartSeconds !== null && gtfsStartSeconds !== null) {
    const diff = Math.abs(paddleStartSeconds - gtfsStartSeconds);
    if (diff <= 5 * 60) score += 6;
    else if (diff <= 10 * 60) score += 3;
  }
  if (paddleEndSeconds !== null && gtfsEndSeconds !== null) {
    const diff = Math.abs(paddleEndSeconds - gtfsEndSeconds);
    if (diff <= 5 * 60) score += 6;
    else if (diff <= 10 * 60) score += 3;
  }

  const paddleHeadsign = normalizeHeadsign(paddleTrip.headSign);
  const gtfsHeadsign = normalizeHeadsign(gtfsTrip.headsign);
  if (paddleHeadsign && gtfsHeadsign && paddleHeadsign === gtfsHeadsign) score += 5;
  else if (paddleHeadsign && gtfsHeadsign && (paddleHeadsign.includes(gtfsHeadsign) || gtfsHeadsign.includes(paddleHeadsign))) score += 2;

  const paddleStartStop = normalizeStopLabel(paddleTrip.startStop);
  const paddleEndStop = normalizeStopLabel(paddleTrip.endStop);
  const gtfsStartStop = normalizeStopLabel(gtfsTrip.firstStopName);
  const gtfsEndStop = normalizeStopLabel(gtfsTrip.lastStopName);

  if (paddleStartStop && gtfsStartStop && paddleStartStop === gtfsStartStop) score += 6;
  if (paddleEndStop && gtfsEndStop && paddleEndStop === gtfsEndStop) score += 6;

  const looksReversed = (
    paddleStartStop &&
    paddleEndStop &&
    gtfsStartStop &&
    gtfsEndStop &&
    paddleStartStop === gtfsEndStop &&
    paddleEndStop === gtfsStartStop
  );
  if (looksReversed) score -= 18;

  const directionHint = normalizeHeadsign(paddleTrip.headSign);
  if (directionHint && gtfsStartStop && gtfsEndStop) {
    const towardEnd = directionHint.includes(gtfsEndStop) || gtfsEndStop.includes(directionHint);
    const towardStart = directionHint.includes(gtfsStartStop) || gtfsStartStop.includes(directionHint);
    if (towardEnd && !towardStart) score += 4;
    if (towardStart && !towardEnd) score -= 4;
  }

  const requestedBlock = normalizeBlockId(context.blockId);
  const gtfsBlock = normalizeBlockId(gtfsTrip.blockId);
  if (requestedBlock && gtfsBlock) {
    if (requestedBlock === gtfsBlock) score += 12;
    else score -= 8;
  }

  return score;
}

function getCandidateGtfsTripsForPaddle(staticIndex, paddleTrip) {
  const route = String(paddleTrip?.routeId || '').trim();
  if (!route) {
    return [...staticIndex.tripsById.values()];
  }
  return staticIndex.tripsByRoute.get(route) || [...staticIndex.tripsById.values()];
}

function rankGtfsTripMatches(paddleTrip, staticIndex, limit = 5, context = {}) {
  return getCandidateGtfsTripsForPaddle(staticIndex, paddleTrip)
    .map((gtfsTrip) => ({
      tripId: gtfsTrip.tripId,
      routeId: gtfsTrip.routeId,
      routeShortName: gtfsTrip.routeShortName,
      headsign: gtfsTrip.headsign,
      firstDeparture: gtfsTrip.firstDeparture,
      lastArrival: gtfsTrip.lastArrival,
      firstStopName: gtfsTrip.firstStopName,
      lastStopName: gtfsTrip.lastStopName,
      blockId: gtfsTrip.blockId,
      score: scoreGtfsTripMatch(paddleTrip, gtfsTrip, context),
    }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

function getTripsForRoute(staticIndex, routeId, limit = 50) {
  const route = String(routeId || '').trim();
  if (!route) return [];
  return (staticIndex.tripsByRoute.get(route) || []).slice(0, limit);
}

async function debugGtfsState(options = {}) {
  if (!isConfigured()) {
    return { ok: false, error: 'GTFS-RT is not configured.' };
  }

  const routeId = String(options.routeId || '').trim();
  const [staticIndex, realtime] = await Promise.all([
    loadStaticIndex(routeId ? [routeId] : []),
    loadRealtimeSnapshot(),
  ]);

  const routeTrips = routeId ? getTripsForRoute(staticIndex, routeId, 200) : [];
  const routeTripIds = new Set(routeTrips.map((trip) => trip.tripId));
  const routeVehicleMatches = [];
  for (const [tripId, vehicleId] of realtime.vehicleByTripId.entries()) {
    if (routeTripIds.size && !routeTripIds.has(tripId)) continue;
    const staticTrip = staticIndex.tripsById.get(tripId);
    routeVehicleMatches.push({
      tripId,
      vehicleId,
      routeId: staticTrip?.routeId || '',
      routeShortName: staticTrip?.routeShortName || '',
      headsign: staticTrip?.headsign || '',
      blockId: staticTrip?.blockId || '',
      firstDeparture: staticTrip?.firstDeparture || '',
      lastArrival: staticTrip?.lastArrival || '',
    });
    if (routeVehicleMatches.length >= 50) break;
  }

  const sampleTrips = routeTrips.slice(0, 25).map((trip) => ({
    tripId: trip.tripId,
    routeId: trip.routeId,
    routeShortName: trip.routeShortName,
    headsign: trip.headsign,
    blockId: trip.blockId,
    firstDeparture: trip.firstDeparture,
    lastArrival: trip.lastArrival,
    firstStopName: trip.firstStopName,
    lastStopName: trip.lastStopName,
    hasVehicleMatch: realtime.vehicleByTripId.has(trip.tripId),
    matchedVehicleId: realtime.vehicleByTripId.get(trip.tripId) || null,
  }));

  return {
    ok: true,
    routeId: routeId || null,
    staticTripCount: staticIndex.tripsById.size,
    realtimeTripVehicleCount: realtime.vehicleByTripId.size,
    realtimePositionCount: realtime.positionsByVehicleId.size,
    sampleTrips,
    routeVehicleMatches,
  };
}

function matchPaddleTripToGtfsTrip(paddleTrip, staticIndex, context = {}) {
  let best = null;
  for (const gtfsTrip of getCandidateGtfsTripsForPaddle(staticIndex, paddleTrip)) {
    const score = scoreGtfsTripMatch(paddleTrip, gtfsTrip, context);
    if (score <= 0) continue;
    if (!best || score > best.score) {
      best = { tripId: gtfsTrip.tripId, score, gtfsTrip };
    }
  }
  return best;
}

function findNearbyVehicleMatchForBlock(paddleTrip, staticIndex, realtime, context = {}) {
  const requestedBlock = normalizeBlockId(context.blockId);
  if (!requestedBlock) return null;

  const paddleStartSeconds = parseServiceTime(paddleTrip.scheduledStartTime);
  const paddleEndSeconds = parseServiceTime(paddleTrip.scheduledEndTime);
  const targetSeconds = paddleStartSeconds ?? paddleEndSeconds;
  const candidates = [];

  for (const [tripId, vehicleId] of realtime.vehicleByTripId.entries()) {
    const gtfsTrip = staticIndex.tripsById.get(tripId);
    if (!gtfsTrip) continue;
    if (normalizeBlockId(gtfsTrip.blockId) !== requestedBlock) continue;

    const startSeconds = parseServiceTime(gtfsTrip.firstDeparture);
    const endSeconds = parseServiceTime(gtfsTrip.lastArrival);
    const compareSeconds = startSeconds ?? endSeconds;
    const timeDiff = targetSeconds !== null && compareSeconds !== null
      ? Math.abs(targetSeconds - compareSeconds)
      : Number.POSITIVE_INFINITY;
    const score = scoreGtfsTripMatch(paddleTrip, gtfsTrip, context);

    candidates.push({
      tripId,
      vehicleId,
      gtfsTrip,
      score,
      timeDiff,
    });
  }

  candidates.sort((a, b) => {
    if (a.timeDiff !== b.timeDiff) return a.timeDiff - b.timeDiff;
    return b.score - a.score;
  });

  return candidates[0] || null;
}

function getNowOttawaSeconds() {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'America/Toronto',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(new Date());
  return parseServiceTime(parts);
}

function selectLikelyPaddleTrips(paddleTrips, nowSecondsOverride = null) {
  const nowSeconds = nowSecondsOverride ?? getNowOttawaSeconds() ?? 0;
  const candidatesBase = paddleTrips
    .filter((trip) => trip && trip.scheduledStartTime && trip.routeId)
    .map((trip) => ({
      trip,
      start: parseServiceTime(trip.scheduledStartTime),
      end: parseServiceTime(trip.scheduledEndTime),
    }))
    .filter((entry) => entry.start !== null);

  if (!candidatesBase.length) return [];

  let dayOffset = 0;
  let previousStart = null;
  const ordered = candidatesBase
    .sort((a, b) => (Number(a.trip.tripNumber) || 0) - (Number(b.trip.tripNumber) || 0))
    .map((entry) => {
      if (previousStart !== null && entry.start < previousStart) {
        dayOffset += 24 * 3600;
      }
      previousStart = entry.start;
      const start = entry.start + dayOffset;
      let end = (entry.end ?? entry.start) + dayOffset;
      if (end < start) end += 24 * 3600;
      return { ...entry, start, end };
    });

  const compareNow = nowSeconds < 4 * 3600 && ordered.some((entry) => entry.end > 24 * 3600)
    ? nowSeconds + 24 * 3600
    : nowSeconds;

  const live = ordered.find((entry) => entry.start <= compareNow && compareNow <= entry.end + 20 * 60);
  if (live) {
    const idx = ordered.indexOf(live);
    return [ordered[idx], ordered[idx - 1], ordered[idx + 1]].filter(Boolean).map((entry) => entry.trip);
  }

  return ordered.slice(0, 3).map((entry) => entry.trip);
}

function realtimeTripDescriptorMatchesStaticTrip(tripDescriptor, staticTrip) {
  if (!tripDescriptor || !staticTrip) return false;
  const realtimeRoute = firstNonEmpty(tripDescriptor.route_id, tripDescriptor.routeId);
  const staticRoutes = new Set([
    String(staticTrip.routeId || '').trim(),
    String(staticTrip.routeShortName || '').trim(),
    String(staticTrip.routeId || '').split('-')[0].trim(),
  ].filter(Boolean));
  return !realtimeRoute || staticRoutes.has(realtimeRoute);
}

function findDirectVehiclePositionsForBlock(block, staticIndex, realtime) {
  const requestedBlock = normalizeBlockId(block);
  if (!requestedBlock) return [];

  const matches = [];
  for (const [vehicleId, position] of realtime.positionsByVehicleId.entries()) {
    const tripId = firstNonEmpty(position?.trip?.trip_id, position?.trip?.tripId);
    if (!tripId) continue;
    const gtfsTrip = staticIndex.tripsById.get(tripId);
    if (!realtimeTripDescriptorMatchesStaticTrip(position.trip, gtfsTrip)) continue;
    if (!gtfsTrip || normalizeBlockId(gtfsTrip.blockId) !== requestedBlock) continue;
    matches.push({
      vehicleId,
      tripId,
      gtfsTrip,
      position,
      timestamp: Number(position.timestamp || 0),
    });
  }

  return matches.sort((a, b) => b.timestamp - a.timestamp);
}

async function lookupBlockWithGtfsRt(block, paddleTrips, options = {}) {
  if (!isConfigured()) {
    return { ok: false, error: 'GTFS-RT is not configured.' };
  }
  const routeIds = [...new Set((paddleTrips || []).map((trip) => String(trip.routeId || '').trim()).filter(Boolean))];
  let [staticIndex, realtime] = await Promise.all([loadStaticIndex(routeIds), loadRealtimeSnapshot()]);
  staticIndex = await refreshStaticForRealtimeMismatch(staticIndex, realtime);
  const candidates = selectLikelyPaddleTrips(paddleTrips, options.nowSeconds ?? null);
  const matches = [];
  const debugCandidates = [];
  const directPositions = findDirectVehiclePositionsForBlock(block, staticIndex, realtime);

  if (directPositions.length) {
    const direct = directPositions[0];
    const paddleTrip = candidates
      .map((trip) => ({ trip, score: scoreGtfsTripMatch(trip, direct.gtfsTrip, { blockId: block }) }))
      .sort((a, b) => b.score - a.score)[0] || null;
    const stopId = firstNonEmpty(direct.position.stop_id, direct.position.stopId);
    matches.push({
      paddleTrip: paddleTrip?.trip || null,
      gtfsTripId: direct.tripId,
      vehicleId: direct.vehicleId,
      score: paddleTrip?.score ?? null,
      vehicleSource: 'vehicle-position-trip',
      position: {
        latitude: direct.position.position?.latitude ?? null,
        longitude: direct.position.position?.longitude ?? null,
        stopId,
        stopName: staticIndex.stopsById.get(stopId)?.name || findNearestStop(direct.position, staticIndex)?.name || '',
        currentStopSequence: direct.position.current_stop_sequence ?? direct.position.currentStopSequence ?? null,
      },
    });
    return {
      ok: true,
      mode: 'block',
      block,
      candidateCount: candidates.length,
      nowSeconds: options.nowSeconds ?? null,
      matches,
      debugCandidates: [{
        directVehiclePosition: true,
        gtfsTripId: direct.tripId,
        gtfsBlockId: direct.gtfsTrip.blockId,
        matchedVehicleId: direct.vehicleId,
        matchedVehicleSource: 'vehicle-position-trip',
      }],
    };
  }

  for (const trip of candidates) {
    const matchContext = { blockId: block };
    const topMatches = rankGtfsTripMatches(trip, staticIndex, 5, matchContext);
    const gtfsMatch = matchPaddleTripToGtfsTrip(trip, staticIndex, matchContext);
    let vehicleId = gtfsMatch ? (realtime.vehicleByTripId.get(gtfsMatch.tripId) || '') : '';
    let vehicleSource = vehicleId ? 'exact-trip' : '';
    let fallbackMatch = null;
    if (gtfsMatch && !vehicleId) {
      fallbackMatch = findNearbyVehicleMatchForBlock(trip, staticIndex, realtime, matchContext);
      if (fallbackMatch?.vehicleId) {
        vehicleId = fallbackMatch.vehicleId;
        vehicleSource = 'same-block-fallback';
      }
    }
    const position = vehicleId ? (realtime.positionsByVehicleId.get(vehicleId) || null) : null;
    debugCandidates.push({
      paddleTrip: {
        tripNumber: trip.tripNumber ?? null,
        routeId: trip.routeId,
        headSign: trip.headSign,
        scheduledStartTime: trip.scheduledStartTime,
        scheduledEndTime: trip.scheduledEndTime,
        startStop: trip.startStop,
        endStop: trip.endStop,
      },
      bestGtfsTripId: gtfsMatch?.tripId || null,
      bestScore: gtfsMatch?.score ?? null,
      matchedVehicleId: vehicleId || null,
      matchedVehicleSource: vehicleSource || null,
      fallbackGtfsTripId: fallbackMatch?.tripId || null,
      topMatches,
    });
    if (!gtfsMatch || !vehicleId) continue;
    matches.push({
      paddleTrip: trip,
      gtfsTripId: fallbackMatch?.tripId || gtfsMatch.tripId,
      vehicleId,
      score: gtfsMatch.score,
      vehicleSource: vehicleSource || null,
      position: position ? {
        latitude: position.position?.latitude ?? null,
        longitude: position.position?.longitude ?? null,
        stopId: firstNonEmpty(position.stop_id, position.stopId),
        stopName: staticIndex.stopsById.get(firstNonEmpty(position.stop_id, position.stopId))?.name || findNearestStop(position, staticIndex)?.name || '',
        currentStopSequence: position.current_stop_sequence ?? position.currentStopSequence ?? null,
      } : null,
    });
  }

  return {
    ok: true,
    mode: 'block',
    block,
    candidateCount: candidates.length,
    nowSeconds: options.nowSeconds ?? null,
    matches,
    debugCandidates,
  };
}

async function lookupBusWithGtfsRt(busNumber, activePaddlesWithTrips) {
  if (!isConfigured()) {
    return { ok: false, error: 'GTFS-RT is not configured.' };
  }
  const routeIds = [...new Set(
    (activePaddlesWithTrips || [])
      .flatMap((candidate) => (candidate.trips || []).map((trip) => String(trip.routeId || '').trim()))
      .filter(Boolean)
  )];
  let [staticIndex, realtime] = await Promise.all([loadStaticIndex(routeIds), loadRealtimeSnapshot()]);
  staticIndex = await refreshStaticForRealtimeMismatch(staticIndex, realtime);
  const normalizedBus = String(busNumber || '').trim();
  const position = realtime.positionsByVehicleId.get(normalizedBus) || null;
  const positionTripId = firstNonEmpty(position?.trip?.trip_id, position?.trip?.tripId);
  let matched = null;

  for (const candidate of activePaddlesWithTrips) {
    for (const trip of candidate.trips || []) {
      const gtfsMatch = matchPaddleTripToGtfsTrip(trip, staticIndex, { blockId: candidate.block });
      if (!gtfsMatch) continue;
      let matchedTripId = gtfsMatch.tripId;
      let vehicleId = realtime.vehicleByTripId.get(gtfsMatch.tripId) || '';
      let vehicleSource = vehicleId ? 'exact-trip' : '';
      if (!vehicleId) {
        const fallbackMatch = findNearbyVehicleMatchForBlock(trip, staticIndex, realtime, { blockId: candidate.block });
        if (fallbackMatch?.vehicleId) {
          vehicleId = fallbackMatch.vehicleId;
          matchedTripId = fallbackMatch.tripId;
          vehicleSource = 'same-block-fallback';
        }
      }
      const matchesByVehicle = vehicleId === normalizedBus;
      const matchesByPositionTrip = positionTripId && matchedTripId && positionTripId === matchedTripId;
      if (!matchesByVehicle && !matchesByPositionTrip) continue;
      matched = {
        block: candidate.block,
        serviceDay: candidate.serviceDay,
        paddleTrip: trip,
        gtfsTripId: matchedTripId,
        score: gtfsMatch.score,
        vehicleSource: vehicleSource || null,
      };
      break;
    }
    if (matched) break;
  }

  return {
    ok: true,
    mode: 'bus',
    busNumber: normalizedBus,
    position: position ? {
      latitude: position.position?.latitude ?? null,
      longitude: position.position?.longitude ?? null,
      stopId: firstNonEmpty(position.stop_id, position.stopId),
      stopName: staticIndex.stopsById.get(firstNonEmpty(position.stop_id, position.stopId))?.name || findNearestStop(position, staticIndex)?.name || '',
      currentStopSequence: position.current_stop_sequence ?? position.currentStopSequence ?? null,
      tripId: positionTripId,
      routeId: firstNonEmpty(position.trip?.route_id, position.trip?.routeId),
    } : null,
    matched,
  };
}

async function lookupBusPositionWithGtfsRt(busNumber) {
  if (!isConfigured()) {
    return { ok: false, error: 'GTFS-RT is not configured.' };
  }

  const realtime = await loadRealtimeSnapshot();
  const normalizedBus = String(busNumber || '').trim();
  const position = realtime.positionsByVehicleId.get(normalizedBus) || null;
  const positionTripId = firstNonEmpty(position?.trip?.trip_id, position?.trip?.tripId);
  const positionRouteId = firstNonEmpty(position?.trip?.route_id, position?.trip?.routeId);
  let staticIndex = await loadStaticIndex(positionRouteId ? [positionRouteId] : []);
  staticIndex = await refreshStaticForRealtimeMismatch(staticIndex, realtime);
  const staticTripCandidate = positionTripId ? (staticIndex.tripsById.get(positionTripId) || null) : null;
  const staticTrip = realtimeTripDescriptorMatchesStaticTrip(position?.trip, staticTripCandidate)
    ? staticTripCandidate
    : null;
  const stopId = firstNonEmpty(position?.stop_id, position?.stopId);
  const stopName = staticIndex.stopsById.get(stopId)?.name || findNearestStop(position, staticIndex)?.name || '';

  return {
    ok: true,
    mode: 'bus-position',
    busNumber: normalizedBus,
    position: position ? {
      latitude: position.position?.latitude ?? null,
      longitude: position.position?.longitude ?? null,
      stopId,
      stopName,
      currentStopSequence: position.current_stop_sequence ?? position.currentStopSequence ?? null,
      tripId: positionTripId,
      routeId: positionRouteId,
      routeShortName: staticTrip?.routeShortName || '',
      headsign: staticTrip?.headsign || '',
      blockId: staticTrip?.blockId || '',
    } : null,
  };
}

async function warmGtfsRtCaches() {
  if (!isConfigured()) {
    warmupStatus = {
      startedAt: null,
      finishedAt: null,
      durationMs: null,
      ok: false,
      error: 'GTFS-RT is not configured.',
    };
    return { ok: false, skipped: true, reason: warmupStatus.error };
  }

  if (warmupPromise) {
    return warmupPromise;
  }

  const startedAt = Date.now();
  warmupStatus = {
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: null,
    durationMs: null,
    ok: false,
    error: '',
  };

  warmupPromise = (async () => {
    try {
      await Promise.all([
        loadStaticIndex(),
        loadRealtimeSnapshot(),
      ]);
      const finishedAt = Date.now();
      warmupStatus = {
        startedAt: warmupStatus.startedAt,
        finishedAt: new Date(finishedAt).toISOString(),
        durationMs: finishedAt - startedAt,
        ok: true,
        error: '',
      };
      return { ok: true, durationMs: warmupStatus.durationMs };
    } catch (error) {
      const finishedAt = Date.now();
      warmupStatus = {
        startedAt: warmupStatus.startedAt,
        finishedAt: new Date(finishedAt).toISOString(),
        durationMs: finishedAt - startedAt,
        ok: false,
        error: String(error?.message || error || 'GTFS warmup failed'),
      };
      throw error;
    } finally {
      warmupPromise = null;
    }
  })();

  return warmupPromise;
}

function getGtfsWarmupStatus() {
  return {
    ...warmupStatus,
    inProgress: Boolean(warmupPromise),
  };
}

module.exports = {
  buildRealtimeIndexes,
  captureRealtimeEvidence,
  findDirectVehiclePositionsForBlock,
  getAvailableBlocks,
  getStaticCacheStatus,
  hasRealtimeTripIdMismatch,
  isConfigured,
  getConfig,
  debugGtfsState,
  getGtfsWarmupStatus,
  loadStaticIndex,
  loadRealtimeSnapshot,
  listAvailableBlocks,
  lookupBlockWithGtfsRt,
  lookupBusWithGtfsRt,
  lookupBusPositionWithGtfsRt,
  warmGtfsRtCaches,
};
