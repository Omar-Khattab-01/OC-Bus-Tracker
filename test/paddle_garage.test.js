'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { getGarageReturn } = require('../lib/paddle_garage');
const winter = require('../data/winter_paddles.index.json').variants.winter.service_days;

test('garage return separates arrival and post-trip checks from the driving directions', () => {
  const run = winter.saturday['098001'];
  const result = getGarageReturn(run.trips.at(-1), run.garage);
  assert.equal(result.garage, 'Industrial Garage');
  assert.equal(result.arrivalTime, '00:50');
  assert.equal(result.durationMinutes, 15);
  assert.match(result.directions, /TRANSITWAY EAST/);
  assert.doesNotMatch(result.directions, /Conduct Post Trip Checks|Garage\s+0:50/);
  assert.match(result.postTripChecks, /Conduct Post Trip Checks/);
});

test('garage deadhead duration handles midnight and extended-hour schedules', () => {
  for (const [end, arrival, expected] of [['23:55', '0:15', 20], ['0:35', '0:50', 15], ['24:50', '25:05', 15], ['12:00', '12:18', 18]]) {
    const result = getGarageReturn({ end_time: end, next_directions: `(L) ROADWAY. Industrial Garage ${arrival}` }, 'Industrial Garage');
    assert.equal(result.durationMinutes, expected, `${end} to ${arrival}`);
  }
});

test('a service trip ending at the garage shows arrival without inventing a deadhead', () => {
  const result = getGarageReturn({ end_stop: 'Merivale Garage', end_time: '14:21' }, 'Merivale Garage');
  assert.equal(result.arrivalTime, '14:21');
  assert.equal(result.isDeadhead, false);
  assert.equal(result.durationMinutes, null);
});

test('missing or invalid garage timing never produces an invented arrival or duration', () => {
  assert.equal(getGarageReturn({ end_time: '1:00', next_directions: '(L) ROADWAY TO DEPOT.' }, 'Industrial Garage'), null);
  assert.equal(getGarageReturn({ end_time: '1:00', next_directions: 'Industrial Garage 1:99' }, 'Industrial Garage'), null);
  const result = getGarageReturn({ next_directions: 'Industrial Garage 1:15' }, 'Industrial Garage');
  assert.equal(result.arrivalTime, '01:15');
  assert.equal(result.durationMinutes, null);
});

test('every winter paddle with a final garage arrival can expose it as structured timing', () => {
  let count = 0;
  for (const runs of Object.values(winter)) {
    for (const run of Object.values(runs)) {
      const result = getGarageReturn(run.trips.at(-1), run.garage);
      assert.ok(result, run.paddle_id);
      assert.match(result.arrivalTime, /^\d{2}:\d{2}$/);
      if (result.isDeadhead) assert.ok(Number.isInteger(result.durationMinutes) && result.durationMinutes >= 0);
      count += 1;
    }
  }
  assert.equal(count, 2446);
});
