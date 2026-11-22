'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');

test('admin status and scheduled actions stay authenticated and cannot be cached', async (t) => {
  process.env.BOOKING_BOARD_ADMIN_TOKEN = 'local-test-only';
  process.env.CRON_SECRET = 'local-cron-only';
  process.env.GTFS_STATIC_GITHUB_TOKEN = '';
  process.env.BOOKING_BOARD_GITHUB_TOKEN = '';
  const app = require('../server');
  app.set('trust proxy', true);
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  assert.equal((await fetch(base + '/api/admin/gtfs-static')).status, 401);
  assert.equal((await fetch(base + '/api/cron/gtfs-static-check', { headers: { 'x-forwarded-for': '192.0.2.1' } })).status, 401);
  const response = await fetch(base + '/api/admin/gtfs-static', { headers: { 'x-admin-token': 'local-test-only' } });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('cache-control'), /no-store/);
  const data = await response.json();
  assert.ok(data.control.index.feedVersion);
  assert.ok(data.control.status.state);
  assert.equal(data.config.githubConfigured, false);
  assert.doesNotMatch(JSON.stringify(data), /local-test-only|local-cron-only/);
  const html = await (await fetch(base + '/gtfs-static-admin')).text();
  assert.match(html, /Official OC Transpo feed/);
  assert.match(html, /Live website index/);
  for (const match of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
  assert.doesNotMatch(html, /Official metadata matches the saved baseline/);
});
