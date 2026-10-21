'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const app = require('../server');

test('holiday tabs and their handlers are removed while current navigation remains', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
  assert.doesNotMatch(html, /(?:mobile)?(?:CanadaDay|CivicHoliday)PaddlesBtn/i);
  assert.doesNotMatch(html, /href=['"]\/(?:canada-day|civic-holiday)-paddles/);
  assert.match(html, /id="mobileFallPaddlesBtn"/);
  assert.match(html, /id="mobileShuttlesPageBtn"/);
});

test('old holiday pages and GET/POST APIs are retired without affecting current pages', async (t) => {
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const origin = 'http://127.0.0.1:' + server.address().port;
  for (const holiday of ['canada-day', 'civic-holiday']) {
    const page = await fetch(origin + '/' + holiday + '-paddles');
    assert.equal(page.status, 410);
    assert.match(await page.text(), /Back to Lookup/);
    for (const method of ['GET', 'POST']) {
      const response = await fetch(origin + '/api/' + holiday + '-paddles', { method });
      assert.equal(response.status, 410);
      assert.equal((await response.json()).ok, false);
      assert.match(response.headers.get('cache-control'), /no-store/);
    }
  }
  for (const page of ['/', '/fall-paddles', '/shuttles', '/support']) {
    const response = await fetch(origin + page);
    assert.equal(response.status, 200, page);
    await response.text();
  }
});
