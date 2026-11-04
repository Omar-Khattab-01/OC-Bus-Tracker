'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { paddle, live } = require('./fixtures/paddle-labels');
const { getGarageReturn } = require('../lib/paddle_garage');

function renderer(file, name, extras = {}) {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', file), 'utf8');
  function declaration(id) {
    const start = html.indexOf('    function ' + id + '(');
    assert.ok(start >= 0, id);
    const next = html.slice(start + 1).search(/\n    (?:async )?function /);
    return html.slice(start, next < 0 ? undefined : start + 1 + next);
  }
  const context = vm.createContext({
    currentUser: null, buildDebugSummary: () => '', getActiveDayTodayWorkEntry: () => null,
    readTodayWork: () => null, renderPostTripCheckNotice: () => '', buildGoogleMapsUrl: () => '',
    splitPostTripCheckText: (text) => ({ directions: text }), ...extras,
  });
  vm.runInContext(declaration('escapeHtml') + '\n' + declaration('renderDirectionBlock') + '\n' + declaration(name), context);
  return context[name];
}

test('active trip prioritizes route and destination over the paddle sequence', () => {
  const render = renderer('index.html', 'buildPaddleSummaryCard', { escapeAttribute: String });
  const html = render(paddle);
  assert.match(html, /summary-main">Route 17 · Wateridge</);
  assert.match(html, /Paddle trip #6/);
  assert.doesNotMatch(html, /Trip 6:/);
  const second = render({ ...paddle, activeTrip: { ...paddle.activeTrip, tripNumber: 9, route: '38', headsign: 'Blair Station' } });
  assert.match(second, /summary-main">Route 38 · Blair Station</);
  assert.match(second, /Paddle trip #9/);
  const escaped = render({ ...paddle, activeTrip: { ...paddle.activeTrip, headsign: '<img src=x>' } });
  assert.doesNotMatch(escaped, /<img/);
});

test('live cards distinguish the route, paddle, and paddle trip number', () => {
  const render = renderer('index.html', 'buildLiveCardsHtml', { escapeAttribute: String });
  const html = render(live);
  assert.match(html, /Currently operating: Route 17 · Wateridge/);
  assert.match(html, /Paddle 44-7 · Paddle trip #6/);
  assert.doesNotMatch(html, /\(Trip 6\)/);
  assert.doesNotMatch(render({ ...live, currentTrip: null }), /Paddle trip #/);
});

test('break summaries distinguish the next route from its paddle trip number', () => {
  const render = renderer('index.html', 'buildPaddleSummaryCard', { escapeAttribute: String });
  const html = render({ isLiveDay: true, activeBreak: { remainingMinutes: 8,
    nextTripNumber: 9, nextTripRoute: '38', nextTripHeadsign: 'Blair Station' } });
  assert.match(html, /Next: Route 38 · Blair Station/);
  assert.match(html, /Paddle trip #9/);
  assert.doesNotMatch(html, /Until Trip/);
});

for (const file of ['index.html', 'summer-booking.html']) {
  test(file + ' gives garage arrival a prominent time and labels the final deadhead duration', () => {
    const run = require('../data/winter_paddles.index.json').variants.winter.service_days.saturday['098001'];
    const trip = { ...run.trips.at(-1) };
    trip.garageReturn = getGarageReturn(trip, run.garage);
    const html = renderer(file, 'renderPaddleTrips')([trip], run.garage, null, null);
    assert.match(html, />Deadhead to Garage<\/div>/);
    assert.match(html, /direction-duration">15 min<\/span>/);
    assert.match(html, /garage-arrival-time" datetime="00:50">00:50<\/time>/);
    assert.match(html, /<strong>Industrial Garage<\/strong>/);
    assert.doesNotMatch(html, /15 min between trips|Industrial Garage 0:50/);
    const direct = { ...trip, next_directions: null, end_stop: 'Industrial Garage', end_time: '01:10' };
    direct.garageReturn = getGarageReturn(direct, run.garage);
    const directHtml = renderer(file, 'renderPaddleTrips')([direct], run.garage, null, null);
    assert.match(directHtml, /garage-arrival-time" datetime="01:10">01:10/);
    assert.doesNotMatch(directHtml, /Deadhead to Garage|direction-duration/);
  });

  test(file + ' shows the garage leg before trip 1 and deadhead with its scheduled gap after it', () => {
    const run = require('../data/winter_paddles.index.json').variants.winter.service_days.weekday['900200'];
    const trips = run.trips.slice(0, 3).map((trip) => ({ ...trip }));
    trips[0].breakAfterMinutes = 24;
    trips[1].next_directions = null;
    trips[1].breakAfterMinutes = 8;
    const render = renderer(file, 'renderPaddleTrips');
    const html = render(trips, run.garage, null,
      { nextTripNumber: trips[1].trip_number, nextTripRoute: trips[1].route });
    const first = html.indexOf('Route 47 · Russell/Hydro');
    const deadhead = html.indexOf('>Deadhead</div>');
    const second = html.indexOf(`Route ${trips[1].route} ·`);
    assert.ok(html.indexOf('>Garage to First Trip</div>') < first);
    assert.ok(first < deadhead && deadhead < second);
    assert.ok(html.indexOf('</article>', first) < deadhead);
    assert.match(html, /Paddle trip #1 · 5:45 to 6:07/);
    assert.match(html, /24 min between trips/);
    assert.doesNotMatch(html, /trip-break-duration">24 min/);
    assert.match(html, /trip-break-label">Break</);
    assert.match(html, /trip-break-duration">8 min/);
    if (file === 'index.html') {
      assert.match(html, /section class="direction-block" id="currentPaddleTarget"/);
      assert.equal((html.match(/id="currentPaddleTarget"/g) || []).length, 1);
    }
  });

  test(file + ' trip list presents the route first and labels the sequence separately', () => {
    const render = renderer(file, 'renderPaddleTrips');
    const html = render(paddle.trips, paddle.garage, paddle.activeTrip, null);
    assert.match(html, /trip-title-main">Route 17 · Wateridge</);
    assert.match(html, /Paddle trip #6 · 17:28 to 17:59/);
    assert.match(html, /Paddle trip #9 · 19:45 to 20:18/);
    assert.match(html, /trip-item active/);
    assert.doesNotMatch(html, /trip-title-prefix">Trip /);
  });
}
