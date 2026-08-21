'use strict';

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const zlib = require('zlib');
const unzipper = require('unzipper');
const { parse } = require('csv-parse/sync');

function parseCsvLine(line) {
  return parse(`${line}\n`, {
    columns: false,
    skip_empty_lines: false,
    relax_quotes: true,
    relax_column_count: true,
  })[0] || [];
}

async function readEntryText(zip, name) {
  const entry = zip.files.find((file) => path.basename(String(file.path || '')) === name);
  if (!entry) throw new Error(`GTFS zip is missing ${name}`);
  return (await entry.buffer()).toString('utf8');
}

async function readEntryRows(zip, name) {
  return parse(await readEntryText(zip, name), {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
  });
}

async function streamStopTimes(zip, tripIds) {
  const entry = zip.files.find((file) => path.basename(String(file.path || '')) === 'stop_times.txt');
  if (!entry) throw new Error('GTFS zip is missing stop_times.txt');

  const boundsByTripId = new Map();
  const rl = readline.createInterface({
    input: entry.stream(),
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
    if (!tripId || !tripIds.has(tripId)) continue;

    const stopSequence = Number(cols[stopSequenceIndex] || 0);
    const arrival = String(cols[arrivalIndex] || '').trim();
    const departure = String(cols[departureIndex] || '').trim();
    const stopId = String(cols[stopIdIndex] || '').trim();
    const current = boundsByTripId.get(tripId) || {
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
    boundsByTripId.set(tripId, current);
  }

  return boundsByTripId;
}

async function main() {
  const sourceZip = process.argv[2];
  const outputFile = process.argv[3] || path.join(process.cwd(), 'data', 'gtfs_static_index.json.gz');
  if (!sourceZip) {
    throw new Error('Usage: node tools/build_gtfs_static_index.js /path/to/GTFSExport.zip [output.json.gz]');
  }

  const zip = await unzipper.Open.file(sourceZip);
  const feedInfoRows = await readEntryRows(zip, 'feed_info.txt').catch(() => []);
  const routeRows = await readEntryRows(zip, 'routes.txt');
  const tripRows = await readEntryRows(zip, 'trips.txt');
  const stopRows = await readEntryRows(zip, 'stops.txt');

  const routeShortNames = new Map();
  for (const row of routeRows) {
    routeShortNames.set(String(row.route_id || '').trim(), String(row.route_short_name || '').trim());
  }

  const stopsById = new Map();
  for (const row of stopRows) {
    const stopId = String(row.stop_id || '').trim();
    if (!stopId) continue;
    stopsById.set(stopId, {
      name: String(row.stop_name || '').trim(),
      latitude: Number(row.stop_lat),
      longitude: Number(row.stop_lon),
    });
  }

  const tripsById = new Map();
  for (const row of tripRows) {
    const tripId = String(row.trip_id || '').trim();
    if (!tripId) continue;
    const routeId = String(row.route_id || '').trim();
    tripsById.set(tripId, {
      routeId,
      routeShortName: routeShortNames.get(routeId) || '',
      serviceId: String(row.service_id || '').trim(),
      headsign: String(row.trip_headsign || '').trim(),
      blockId: String(row.block_id || '').trim(),
    });
  }

  const stopBoundsByTripId = await streamStopTimes(zip, new Set(tripsById.keys()));
  const trips = [];
  for (const [tripId, trip] of tripsById.entries()) {
    const bounds = stopBoundsByTripId.get(tripId);
    if (!bounds) continue;
    trips.push([
      tripId,
      trip.routeId,
      trip.routeShortName,
      trip.serviceId,
      trip.headsign,
      trip.blockId,
      bounds.firstStopId,
      bounds.lastStopId,
      stopsById.get(bounds.firstStopId)?.name || '',
      stopsById.get(bounds.lastStopId)?.name || '',
      bounds.firstDeparture,
      bounds.lastArrival,
    ]);
  }

  const stops = Array.from(stopsById.entries()).map(([stopId, stop]) => [
    stopId,
    stop.name,
    stop.latitude,
    stop.longitude,
  ]);

  const payload = {
    meta: {
      generatedAt: new Date().toISOString(),
      sourceZip: path.basename(sourceZip),
      feedInfo: feedInfoRows[0] || null,
      tripCount: trips.length,
      stopCount: stops.length,
    },
    trips,
    stops,
  };

  fs.mkdirSync(path.dirname(outputFile), { recursive: true });
  fs.writeFileSync(outputFile, zlib.gzipSync(Buffer.from(JSON.stringify(payload)), { level: 9 }));
  console.log(JSON.stringify({
    ok: true,
    outputFile,
    tripCount: trips.length,
    stopCount: stops.length,
    bytes: fs.statSync(outputFile).size,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
