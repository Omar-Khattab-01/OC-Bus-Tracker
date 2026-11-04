'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const http = require('node:http');
const season = require('../public/paddle-season');
const winter = require('../data/winter_paddles.index.json');
const app = require('../server');

test('the production /winter-paddles rewrite serves the dedicated preview and redirects after activation', async (t) => {
  const config = require('../vercel.json');
  assert.equal(config.rewrites.find((rule) => rule.source === '/winter-paddles')?.destination, '/api/winter-paddles');
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-03T17:00:00Z') });
  const server = http.createServer(require('../api/winter-paddles'));
  server.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const origin = `http://127.0.0.1:${server.address().port}`;
  let response = await fetch(origin + '/api/winter-paddles');
  assert.equal(response.status, 200);
  assert.match(response.headers.get('cache-control'), /no-store/);
  const html = await response.text();
  assert.match(html, /isWinterPaddlesPage/);
  assert.match(html, /Starts December 20, 2026/);
  t.mock.timers.setTime(Date.parse('2026-12-20T05:00:00Z'));
  response = await fetch(origin + '/api/winter-paddles', { redirect: 'manual' });
  assert.equal(response.status, 302);
  assert.equal(response.headers.get('location'), '/');
});

test('winter activates at Ottawa midnight, and weekday service respects PDF effective dates', () => {
  assert.equal(season.isWinterActive(new Date('2026-12-20T04:59:59Z')), false);
  assert.equal(season.isWinterActive(new Date('2026-12-20T05:00:00Z')), true);
  assert.equal(season.winterServiceDay('weekday', new Date('2026-12-21T17:00:00Z')), 'reduced_week');
  assert.equal(season.winterServiceDay('weekday', new Date('2027-01-02T17:00:00Z')), 'reduced_week');
  assert.equal(season.winterServiceDay('weekday', new Date('2027-01-04T05:00:00Z')), 'weekday');
  assert.equal(season.winterServiceDay('saturday', new Date('2026-12-26T17:00:00Z')), 'saturday');
  for (const day of ['2026-12-25', '2027-01-01', '2027-03-26']) {
    assert.equal(season.winterServiceDay('weekday', new Date(`${day}T17:00:00Z`)), 'sunday', day);
  }
  assert.equal(season.winterServiceDay('weekday', new Date('2026-12-28T17:00:00Z')), 'special_service');
  for (const day of ['2027-03-15', '2027-03-19', '2027-03-29']) {
    assert.equal(season.winterServiceDay('weekday', new Date(`${day}T17:00:00Z`)), 'reduced_week', day);
  }
  for (const day of ['2027-03-12', '2027-03-22', '2027-03-30']) {
    assert.equal(season.winterServiceDay('weekday', new Date(`${day}T17:00:00Z`)), 'weekday', day);
  }
});

test('winter navigation hides at activation using the same Ottawa date policy', () => {
  const elements = [{ hidden: false }, { hidden: false }];
  let timer;
  class CutoverDate extends Date { constructor(...args) { super(...(args.length ? args : ['2026-12-20T05:00:00Z'])); } }
  const context = {
    Date: CutoverDate, Intl, window: {},
    document: { createElement: () => ({}), head: { appendChild() {} },
      querySelectorAll: () => elements, addEventListener() {} },
    setInterval: (callback) => { timer = callback; },
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../public/paddle-season.js'), 'utf8'), context);
  assert.ok(elements.every((element) => element.hidden));
  assert.equal(typeof timer, 'function');
});

test('all winter source PDFs were imported with usable trips and weekday AM/PM blocks', () => {
  assert.equal(Object.keys(winter.sources).length, 7);
  for (const source of Object.values(winter.sources)) assert.ok(source.parsed_runs > 0);
  const days = winter.variants.winter.service_days;
  let sourcePages = 0;
  let multiPagePaddles = 0;
  for (const [day, count] of Object.entries({ weekday: 876, saturday: 437, sunday: 385, reduced_week: 748 })) {
    assert.equal(Object.keys(days[day]).length, count, day);
    for (const run of Object.values(days[day])) {
      sourcePages += run.source_page_count;
      if (run.source_page_count > 1) multiPagePaddles += 1;
      assert.equal(run.source_page_end, run.page_number + run.source_page_count - 1);
      const sequenceNeedsReview = run.trips.some((trip, index) => trip.trip_number !== index + 1);
      assert.equal(Boolean(run.parse_warnings?.length), sequenceNeedsReview, `${day} ${run.paddle_id}`);
      assert.ok(run.trips.length > 0, run.paddle_id);
      for (const trip of run.trips) assert.ok(trip.start_time && trip.end_time, `${run.paddle_id} trip ${trip.trip_number}`);
    }
  }
  assert.equal(sourcePages, 3113);
  assert.equal(multiPagePaddles, 667);
  assert.equal(days.weekday['900200'].category, 'express_900');
  const numericStop = days.saturday['019005'].trips.find((trip) => trip.trip_number === 5);
  assert.equal(numericStop.headsign, '925 Belfast');
  assert.equal(numericStop.end_time, '1:19');
  assert.equal(days.weekday['031006'].trips[10].trip_number, 11);
  assert.equal(days.weekday['031006'].trips[10].headsign, '3940 Innes Rd');
  assert.equal(days.weekday['900705'].trips[10].trip_number, 11);
  assert.match(days.sunday['098001'].parse_warnings[0], /Trip numbering needs manual review/);
  assert.equal(days.sunday['098001'].page_number, 645);
  assert.equal(days.sunday['098001'].source_page_end, 646);
  assert.equal(days.sunday['098001'].source_page_count, 2);
});

test('winter lookup serves actual distinct schedules, validates input, and never marks future trips live', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-03T17:00:00Z') });
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const request = async (url, init) => {
    const response = await fetch(origin + url, init);
    return { status: response.status, data: await response.json() };
  };
  const result = await request('/api/winter-booking?block=5-01');
  assert.equal(result.status, 200);
  assert.deepEqual(new Set(result.data.paddleOptions.map((option) => option.serviceDay)),
    new Set(['weekday', 'saturday', 'sunday', 'reduced_week']));
  assert.equal(result.data.paddle.paddleId, '005001');
  assert.equal(result.data.paddle.isLiveDay, false);
  assert.equal(result.data.paddle.activeTrip, null);
  const multiPage = await request('/api/winter-booking?block=6-1&day=weekday');
  assert.equal(multiPage.data.paddle.trips.at(-1).trip_number, 19);
  assert.equal(multiPage.data.paddle.trips.at(-1).end_time, '0:54');
  assert.equal(multiPage.data.paddle.sourceWarning, null);
  const roadway = await request('/api/winter-booking?block=10-2&day=weekday');
  assert.equal(roadway.data.paddle.trips[0].end_time, '4:44');
  const garage = await request('/api/winter-booking?block=74-9&day=weekday');
  assert.equal(garage.data.paddle.trips.at(-1).end_time, '17:35');
  assert.match(garage.data.paddle.trips.at(-1).next_directions, /INTO GARAGE/);
  const overnightGarage = await request('/api/winter-booking?block=98-1&day=saturday');
  assert.equal(overnightGarage.data.paddle.trips.at(-1).garageReturn.arrivalTime, '00:50');
  assert.equal(overnightGarage.data.paddle.trips.at(-1).garageReturn.durationMinutes, 15);
  const saturday = await request('/api/winter-booking?block=5-01&day=sat');
  assert.equal(saturday.data.paddle.effective, 'December 26, 2026');
  assert.notDeepEqual(result.data.paddle.trips, saturday.data.paddle.trips);
  const reduced = await request('/api/winter-booking?block=5-01&day=reduced_week');
  assert.equal(reduced.data.paddle.sourceId, 'winter_reduced_low');
  const amPm = await request('/api/winter-booking', { method: 'POST',
    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: '900-200' }) });
  assert.equal(amPm.data.paddle.sourceId, 'winter_weekday_am_pm');
  assert.deepEqual(amPm.data.paddleOptions.map((option) => option.serviceDay), ['weekday']);
  assert.equal((await request('/api/winter-booking?block=999-999')).status, 404);
  assert.equal((await request('/api/winter-booking?block=5-01&day=invalid')).status, 400);
  assert.equal((await request('/api/winter-booking?block=bad')).status, 400);
  assert.equal((await request('/api/paddle?block=5-01&day=weekday')).data.variantId, 'fall');
  const sourceWarning = await request('/api/winter-booking?block=98-1&day=sunday');
  assert.match(sourceWarning.data.paddle.sourceWarning, /Trip numbering needs manual review/);
  assert.deepEqual(sourceWarning.data.paddle.trips, []);
  assert.equal(sourceWarning.data.paddle.sourcePdfUrl, undefined);
  assert.equal(result.data.paddle.sourcePdfUrl, undefined);
  for (const file of ['index.html', 'summer-booking.html']) {
    const html = fs.readFileSync(path.join(__dirname, '../public', file), 'utf8');
    assert.doesNotMatch(html, /Open Original PDF|sourcePdfUrl/);
  }
});

test('regular lookup and saved old variants switch to winter without falling back to retired paddles', async (t) => {
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-12-20T17:00:00Z') });
  let response = await fetch(origin + '/api/paddle?block=5-01&day=sunday&variant=fall');
  assert.equal((await response.json()).variantId, 'winter');
  response = await fetch(origin + '/winter-paddles', { redirect: 'manual' });
  assert.equal(response.status, 302);
  assert.equal(response.headers.get('location'), '/');
  t.mock.timers.setTime(Date.parse('2026-12-21T17:00:00Z'));
  response = await fetch(origin + '/api/paddle?block=5-01&day=weekday&variant=fall');
  const reduced = await response.json();
  assert.equal(reduced.variantId, 'winter');
  assert.equal(reduced.serviceDay, 'reduced_week');
  for (const date of ['2026-12-25', '2027-01-01', '2027-03-26']) {
    t.mock.timers.setTime(Date.parse(`${date}T17:00:00Z`));
    response = await fetch(origin + '/api/paddle?block=5-01&day=weekday&variant=fall');
    assert.equal((await response.json()).serviceDay, 'sunday', date);
  }
  t.mock.timers.setTime(Date.parse('2026-12-28T17:00:00Z'));
  for (const suffix of ['', '&day=weekday']) {
    response = await fetch(origin + '/api/paddle?block=5-01' + suffix);
    assert.equal(response.status, 404);
    assert.match((await response.json()).error, /special service/i);
  }
  t.mock.timers.setTime(Date.parse('2027-03-29T17:00:00Z'));
  response = await fetch(origin + '/api/paddle?block=5-01&day=weekday');
  assert.equal((await response.json()).serviceDay, 'reduced_week');
  t.mock.timers.setTime(Date.parse('2027-01-04T17:00:00Z'));
  response = await fetch(origin + '/api/paddle?block=5-01&day=weekday&variant=fall');
  const regular = await response.json();
  assert.equal(regular.variantId, 'winter');
  assert.equal(regular.serviceDay, 'weekday');
  assert.equal(regular.effective, 'January 04, 2027');
  // This block exists in fall on Sunday but is absent from winter Sunday.
  const fall = require('../data/paddles.index.json').variants.fall.service_days.sunday;
  const absent = Object.keys(fall).find((id) => !winter.variants.winter.service_days.sunday[id]);
  assert.ok(absent);
  const block = `${Number(absent.slice(0, 3))}-${Number(absent.slice(3))}`;
  response = await fetch(origin + `/api/paddle?block=${block}&day=sunday&variant=fall`);
  assert.equal(response.status, 404);
});
