'use strict';

/*
Run a block lookup using only OC Transpo's official GTFS static and
GTFS-Realtime feeds configured through the OCTRANSPO_GTFS_* variables.

  node track_block.js "44-07"
*/

const {
  isConfigured,
  lookupBlockWithGtfsRt,
} = require('./lib/gtfs_rt_runtime');

const EXIT_SUCCESS = 0;
const EXIT_EXPECTED = 2;
const EXIT_UNEXPECTED = 1;

class ExpectedFailure extends Error {
  constructor(message, step, busNumber) {
    super(message);
    this.name = 'ExpectedFailure';
    this.step = step;
    this.busNumber = busNumber || null;
  }
}

function normalizeBlock(value) {
  return String(value || '').trim().toUpperCase().replace(/\s*-\s*/g, '-');
}

function buildLocation(match) {
  const position = match?.position || {};
  const latitude = Number(position.latitude);
  const longitude = Number(position.longitude);
  const stopName = String(position.stopName || '').trim();
  const route = String(match?.gtfsTrip?.routeShortName || match?.paddleTrip?.routeId || '').trim();
  const headsign = String(match?.gtfsTrip?.headsign || match?.paddleTrip?.headSign || '').trim();
  const routeText = [route ? `route ${route}` : '', headsign ? `toward ${headsign}` : '']
    .filter(Boolean)
    .join(' ');

  return {
    busNumber: String(match?.vehicleId || '').trim(),
    locationText: stopName ? `at ${stopName}` : (routeText || 'Location available in OC Transpo GTFS-Realtime'),
    latitude: Number.isFinite(latitude) ? Number(latitude.toFixed(6)) : null,
    longitude: Number.isFinite(longitude) ? Number(longitude.toFixed(6)) : null,
  };
}

async function trackBlock(blockArg) {
  const block = normalizeBlock(blockArg);
  if (!/^\d{1,3}-\d{1,3}$/.test(block)) {
    throw new ExpectedFailure('Usage: node track_block.js "44-07"', 'input');
  }
  if (!isConfigured()) {
    throw new ExpectedFailure('Official OC Transpo GTFS feeds are not configured.', 'configuration');
  }

  const payload = await lookupBlockWithGtfsRt(block, []);
  const seen = new Set();
  const buses = [];
  for (const match of payload?.matches || []) {
    const vehicleId = String(match?.vehicleId || '').trim();
    if (!vehicleId || seen.has(vehicleId)) continue;
    seen.add(vehicleId);
    buses.push(buildLocation(match));
  }

  if (!buses.length) {
    throw new ExpectedFailure(`No live OC Transpo GTFS-Realtime vehicle found for block ${block}.`, 'gtfs-rt');
  }
  return { block, buses, source: 'oc-transpo-gtfs-rt' };
}

async function main() {
  try {
    const result = await trackBlock(process.argv[2]);
    process.stdout.write(JSON.stringify(result));
    process.exit(EXIT_SUCCESS);
  } catch (err) {
    if (err instanceof ExpectedFailure) {
      console.error(err.message);
      process.exit(EXIT_EXPECTED);
      return;
    }
    console.error(`Unexpected error: ${err && err.message ? err.message : String(err)}`);
    process.exit(EXIT_UNEXPECTED);
  }
}

if (require.main === module) {
  main();
}

module.exports = { trackBlock, ExpectedFailure };
