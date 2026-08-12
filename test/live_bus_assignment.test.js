'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  canUseRetainedAssignmentForPosition,
  isRetainableAssignment,
  maskLocationForScheduledBreak,
  selectNewestAssignments,
} = require('../lib/live_bus_assignment');

const now = new Date('2026-08-12T16:00:00.000Z');

test('a confirmed same-day assignment survives an inter-trip break', () => {
  assert.equal(isRetainableAssignment({
    block: '11-02',
    verifiedAt: '2026-08-12T14:30:00.000Z',
  }, { now, afterFinalTrip: false }), true);
});

test('an assignment expires when the paddle work is finished', () => {
  assert.equal(isRetainableAssignment({
    block: '11-02',
    verifiedAt: '2026-08-12T14:30:00.000Z',
  }, { now, afterFinalTrip: true }), false);
});

test('a previous-day assignment is retained only for overnight carryover', () => {
  const mapping = {
    block: '11-02',
    verifiedAt: '2026-08-11T23:30:00.000-04:00',
  };
  assert.equal(isRetainableAssignment(mapping, { now, paddleCarryover: true }), true);
  assert.equal(isRetainableAssignment(mapping, { now, paddleCarryover: false }), false);
});

test('the newest confirmed bus assignment wins after a swap', () => {
  const selected = selectNewestAssignments([
    { busNumber: '4701', block: '11-02', verifiedAt: '2026-08-12T14:00:00.000Z' },
    { busNumber: '4744', block: '11-02', verifiedAt: '2026-08-12T15:00:00.000Z' },
  ], { now });

  assert.deepEqual(selected.map((mapping) => mapping.busNumber), ['4744']);
});

test('a tripless vehicle position is treated as a break on the retained block', () => {
  assert.equal(canUseRetainedAssignmentForPosition(
    { block: '11-02' },
    { tripId: '', blockId: '' }
  ), true);
});

test('a new official block assignment overrides the retained block', () => {
  assert.equal(canUseRetainedAssignmentForPosition(
    { block: '11-02' },
    { tripId: 'new-trip', blockId: '61-04' }
  ), false);
});

test('a scheduled break hides retained live coordinates and uses only the break label', () => {
  assert.deepEqual(maskLocationForScheduledBreak({
    busNumber: '4744',
    locationText: 'Near Terry Fox Station',
    latitude: 45.301,
    longitude: -75.91,
    assignmentStatus: 'confirmed',
  }, true), {
    busNumber: '4744',
    locationText: 'On break',
    latitude: null,
    longitude: null,
    assignmentStatus: 'break',
  });
});

test('a non-break response keeps its live location', () => {
  const bus = {
    busNumber: '4744',
    locationText: 'Near Terry Fox Station',
    latitude: 45.301,
    longitude: -75.91,
  };
  assert.equal(maskLocationForScheduledBreak(bus, false), bus);
});
