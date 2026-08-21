'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildRealtimeIndexes,
  findDirectVehiclePositionsForBlock,
  hasRealtimeTripIdMismatch,
  listAvailableBlocks,
} = require('../lib/gtfs_rt_runtime');

function tripUpdate(tripId, vehicleId) {
  return {
    tripUpdate: {
      trip: { tripId },
      vehicle: vehicleId ? { id: vehicleId } : null,
    },
  };
}

function vehiclePosition(vehicleId, tripId = '', routeId = '') {
  return {
    vehicle: {
      vehicle: { id: vehicleId },
      trip: tripId ? { tripId, routeId } : null,
      position: { latitude: 45.4, longitude: -75.7 },
    },
  };
}

test('VehiclePositions overrides a conflicting TripUpdates vehicle assignment', () => {
  const realtime = buildRealtimeIndexes(
    { entity: [tripUpdate('13643070', '6502')] },
    { entity: [vehiclePosition('6502'), vehiclePosition('4830', '13643070', '53')] }
  );
  const staticIndex = {
    tripsById: new Map([['13643070', {
      tripId: '13643070',
      routeId: '53',
      routeShortName: '53',
      blockId: '53-03',
    }]]),
  };
  const direct = findDirectVehiclePositionsForBlock('53-03', staticIndex, realtime);

  assert.equal(direct[0].vehicleId, '4830');
  assert.equal(realtime.positionsByVehicleId.has('6502'), true);
  assert.equal(realtime.positionsByVehicleId.has('4830'), true);
});

test('block matching treats unpadded and padded OC block IDs as equivalent', () => {
  const realtime = buildRealtimeIndexes(
    { entity: [] },
    { entity: [vehiclePosition('6502', 'trip-44-7', '44')] }
  );
  const staticIndex = {
    tripsById: new Map([['trip-44-7', {
      tripId: 'trip-44-7',
      routeId: '44',
      routeShortName: '44',
      blockId: '44-07',
    }]]),
    stopsById: new Map(),
  };
  const direct = findDirectVehiclePositionsForBlock('44-7', staticIndex, realtime);

  assert.equal(direct[0].vehicleId, '6502');
});

test('an internally inconsistent VehiclePosition trip is rejected', () => {
  const realtime = buildRealtimeIndexes(
    { entity: [] },
    { entity: [vehiclePosition('6634', '15298070', '6')] }
  );
  const staticIndex = {
    tripsById: new Map([['15298070', {
      tripId: '15298070',
      routeId: '56',
      routeShortName: '56',
      blockId: '53-03',
    }]]),
  };

  assert.deepEqual(findDirectVehiclePositionsForBlock('53-03', staticIndex, realtime), []);
});

test('TripUpdates remains the fallback when no VehiclePosition claims the trip', () => {
  const realtime = buildRealtimeIndexes(
    { entity: [tripUpdate('trip-2', '7001')] },
    { entity: [vehiclePosition('7001')] }
  );

  assert.equal(realtime.vehicleByTripId.get('trip-2'), '7001');
});

test('a VehiclePosition without a trip does not create a trip assignment', () => {
  const realtime = buildRealtimeIndexes(
    { entity: [] },
    { entity: [vehiclePosition('6502')] }
  );

  assert.equal(realtime.vehicleByTripId.size, 0);
  assert.equal(realtime.positionsByVehicleId.has('6502'), true);
});

test('available blocks come from official GTFS static trip block IDs', () => {
  const staticIndex = {
    tripsById: new Map([
      ['trip-1', { blockId: '44-07' }],
      ['trip-2', { blockId: '6-2' }],
      ['trip-3', { blockId: '44-07' }],
      ['trip-4', { blockId: '' }],
    ]),
  };

  assert.deepEqual(listAvailableBlocks(staticIndex), ['6-2', '44-07']);
});

test('a realtime trip ID missing from static GTFS triggers mismatch detection', () => {
  const staticIndex = {
    tripsById: new Map([['known-trip', { blockId: '44-07' }]]),
  };
  const matchingRealtime = buildRealtimeIndexes(
    { entity: [tripUpdate('known-trip', '6590')] },
    { entity: [vehiclePosition('6590', 'known-trip', '44')] }
  );
  const changedRealtime = buildRealtimeIndexes(
    { entity: [tripUpdate('new-friday-trip', '6590')] },
    { entity: [vehiclePosition('6590', 'new-friday-trip', '44')] }
  );

  assert.equal(hasRealtimeTripIdMismatch(staticIndex, matchingRealtime), false);
  assert.equal(hasRealtimeTripIdMismatch(staticIndex, changedRealtime), true);
});
