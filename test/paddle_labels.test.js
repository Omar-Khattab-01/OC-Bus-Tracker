'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { paddle, live } = require('./fixtures/paddle-labels');

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
    readTodayWork: () => null, renderPostTripCheckNotice: () => '', renderDirectionBlock: () => '',
    splitPostTripCheckText: (text) => ({ directions: text }), ...extras,
  });
  vm.runInContext(declaration('escapeHtml') + '\n' + declaration(name), context);
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
