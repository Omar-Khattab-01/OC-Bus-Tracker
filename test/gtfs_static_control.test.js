'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const zlib = require('node:zlib');
const { promisify } = require('node:util');
const execFile = promisify(require('node:child_process').execFile);
const { ZIP, harness } = require('./fixtures/gtfs_static_admin');
const { createGtfsStaticAdmin } = require('../lib/gtfs_static_admin');
const { comparison, sameRemote, dueSchedule, sanitizeSchedule, inspectFeed, downloadOfficial, indexMeta, builtControl, DEFAULT_SCHEDULE } = require('../lib/gtfs_static_control');

test('checking twice cannot clear an outstanding update or replace the built baseline', async () => {
  const h = harness();
  const first = await h.service.check();
  const second = await h.service.check();
  assert.equal(first.control.status.state, 'update_available');
  assert.equal(second.control.status.state, 'update_available');
  assert.equal(h.control.official.feedVersion, 'S1000528');
  assert.equal(h.control.index.feedVersion, 'S1000527');
  assert.equal(h.control.index.sourceRemote.sha256, 'old');
  assert.equal(h.inspectCount, 1);
});

test('a scheduled check detects an update even after a manual check saved its metadata', async () => {
  const h = harness();
  await h.service.check();
  await h.service.cron();
  const repeat = await h.service.cron();
  assert.equal(h.calls.filter((x) => x.method === 'POST').length, 1);
  assert.equal(repeat.skipped, true);
  assert.equal(h.control.lastScheduledCheckKey, '2026-11-22T02:00');
});

test('matching versions alone or missing fingerprints cannot produce a green status', () => {
  const h = harness();
  const official = { ...h.feed, remote: h.remote, verifiedAt: '2026-11-22T17:00:00Z' };
  const index = { ...h.deployed, ...h.feed };
  assert.equal(comparison({ official, remote: h.remote }, index).state, 'update_available');
  assert.equal(comparison({ official, remote: h.remote }, { ...index, sourceRemote: null }).state, 'update_available');
  assert.equal(comparison({}, index).state, 'unverified');
  assert.equal(sameRemote(null, h.remote), null);
});

test('green requires the verified official ZIP to match the actual deployed version and source', async () => {
  const h = harness();
  await h.service.check();
  h.setDeployed({ ...h.deployed, ...h.feed, sourceRemote: h.remote });
  assert.equal((await h.service.status()).control.status.state, 'up_to_date');
  h.setTime('2026-11-24T18:00:00Z');
  assert.equal((await h.service.status()).control.status.state, 'unverified');
});

test('official ZIP contents are reverified each Ottawa day, even less than 24 hours after a manual check', async () => {
  const h = harness();
  await h.service.check();
  h.setTime('2026-11-23T07:00:00Z');
  await h.service.check();
  assert.equal(h.inspectCount, 2);
});

test('inconsistent HEAD and GET responses never mark the index current or consume a scheduled check', async () => {
  const h = harness({ options: { head: async () => ({ remote: { etag: 'a-different-CDN-version' } }) } });
  await assert.rejects(h.service.cron(), /metadata and the downloaded ZIP disagree/);
  assert.equal(h.control.lastScheduledCheckKey, '2026-11-20T02:20');
  assert.equal(h.calls.filter((x) => x.method === 'POST').length, 0);
  assert.equal((await h.service.status()).control.status.state, 'unverified');
});

test('simultaneous rebuild clicks share one dispatch, and a recent request prevents duplicates', async () => {
  const h = harness();
  await Promise.all([h.service.rebuild(), h.service.rebuild()]);
  await h.service.rebuild();
  assert.equal(h.calls.filter((x) => x.method === 'POST').length, 1);
});

test('an existing GitHub workflow prevents another dispatch', async () => {
  const h = harness();
  h.setRuns([{ status: 'in_progress', html_url: 'https://github.com/test/repo/actions/runs/1' }]);
  assert.equal((await h.service.rebuild()).alreadyRunning, true);
  assert.equal(h.calls.filter((x) => x.method === 'POST').length, 0);
});

test('independent server instances use an atomic claim to prevent duplicate dispatches', async () => {
  const h = harness();
  const second = createGtfsStaticAdmin(h.options);
  await Promise.all([h.service.rebuild(), second.rebuild()]);
  assert.equal(h.calls.filter((x) => x.method === 'POST').length, 1);
});

test('compare-and-swap retries preserve concurrent builds and schedule changes', async () => {
  const h = harness();
  h.setConflict((control) => { control.index = { feedVersion: 'concurrent-build' }; control.schedule = DEFAULT_SCHEDULE; });
  await h.service.check();
  assert.equal(h.control.index.feedVersion, 'concurrent-build');
  assert.deepEqual(h.control.schedule, DEFAULT_SCHEDULE);
});

test('a slow version check records completion time so an overlapping older build cannot erase it', async () => {
  let h;
  h = harness({ options: { download: async () => {
    h.setTime('2026-11-22T18:03:00Z');
    return { remote: h.remote, feed: h.feed };
  } } });
  await h.service.check();
  assert.equal(h.control.lastCheckedAt, '2026-11-22T18:03:00.000Z');
  assert.equal(h.control.official.verifiedAt, '2026-11-22T18:03:00.000Z');
  const olderBuild = { ...h.deployed, sourceRemote: { ...h.oldRemote, downloadedAt: '2026-11-22T18:01:00Z' } };
  assert.equal(builtControl(h.control, olderBuild).official.feedVersion, 'S1000528');
});

test('schedule saves do not clear verification, errors, or outstanding updates', async () => {
  const h = harness();
  await h.service.check();
  const saved = await h.service.save({ enabled: true, schedule: DEFAULT_SCHEDULE });
  assert.equal(saved.control.status.state, 'update_available');
  assert.equal(saved.control.official.feedVersion, 'S1000528');
  assert.equal(saved.control.index.feedVersion, 'S1000527');
  assert.throws(() => sanitizeSchedule([{ days: ['sunday'], time: '25:00' }]));
});

test('workflow failures and deployment failures stay distinct from a successful build', async () => {
  const h = harness();
  await h.service.check();
  await h.service.rebuild();
  const run = { created_at: '2026-11-22T18:00:01Z', status: 'completed', conclusion: 'failure', html_url: 'https://github.com/test/repo/actions/runs/1' };
  h.setRuns([run]);
  assert.equal((await h.service.status()).control.status.label, 'Rebuild failed');
  h.setRuns([{ ...run, conclusion: 'success' }]);
  assert.equal((await h.service.status()).control.status.state, 'deployment_pending');
  h.setDeploymentStatus('failure');
  assert.equal((await h.service.status()).control.status.label, 'Deployment failed');
  h.setDeployed({ ...h.feed, sourceRemote: h.remote, generatedAt: '2026-11-22T18:02:00Z' });
  assert.equal((await h.service.status()).control.status.state, 'up_to_date');
});

test('failed downloads are visible and leave scheduled checks eligible for a delayed retry', async () => {
  const h = harness();
  h.failHead();
  await assert.rejects(h.service.cron(), /Official GTFS unavailable/);
  assert.equal(h.control.lastScheduledCheckKey, '2026-11-20T02:20');
  assert.equal((await h.service.status()).control.status.state, 'unverified');
  assert.equal(dueSchedule(h.control, new Date('2026-11-22T18:05:00Z')), null);
  assert.ok(dueSchedule(h.control, new Date('2026-11-22T18:16:00Z')));
});

test('failed dispatches do not consume the daily check or silently hide the failure', async () => {
  const h = harness();
  h.failDispatch();
  await assert.rejects(h.service.cron(), /dispatch unavailable/);
  assert.equal(h.control.lastRebuildRequestStatus, 'dispatch-failed');
  assert.equal(h.control.lastScheduledCheckKey, '2026-11-20T02:20');
  assert.equal((await h.service.status()).control.status.state, 'failed');
});

test('realtime coverage reports a mismatch even if official and deployed versions agree', async () => {
  const h = harness({ options: { verifyRealtime: async () => ({ withTrip: 100, matched: 10, matchRate: 0.1 }) } });
  h.setDeployed({ ...h.deployed, ...h.feed, sourceRemote: h.remote });
  const result = await h.service.check();
  assert.equal(result.control.status.state, 'live_mismatch');
});

test('daily checks use Ottawa time, catch up missed minutes, and survive DST transitions', () => {
  const control = { enabled: true, schedule: DEFAULT_SCHEDULE };
  assert.equal(dueSchedule(control, new Date('2026-11-23T06:59:00Z')), null);
  assert.equal(dueSchedule(control, new Date('2026-11-23T07:00:00Z')).current.key, '2026-11-23T02:00');
  assert.ok(dueSchedule(control, new Date('2026-11-23T07:07:00Z')));
  assert.equal(dueSchedule({ ...control, lastScheduledCheckKey: '2026-11-23T02:00' }, new Date('2026-11-23T08:00:00Z')), null);
  assert.equal(dueSchedule(control, new Date('2027-03-14T07:00:00Z')).current.key, '2027-03-14T02:00');
  assert.equal(dueSchedule(control, new Date('2027-07-01T06:00:00Z')).current.key, '2027-07-01T02:00');
});

test('download verification reads the ZIP and binds its actual GET metadata and SHA-256', async () => {
  const calls = [];
  const result = await downloadOfficial(async (url, init) => {
    calls.push({ url, init });
    return new Response(ZIP, { headers: { etag: 'get-etag', 'last-modified': 'get-date', 'content-length': String(ZIP.length) } });
  });
  assert.equal(result.feed.feedVersion, 'S1000528');
  assert.equal(result.remote.etag, 'get-etag');
  assert.equal(result.remote.sha256, crypto.createHash('sha256').update(ZIP).digest('hex'));
  assert.equal(calls[0].init.method, 'GET');
  await assert.rejects(inspectFeed(Buffer.from('not a ZIP')));
  let attempts = 0;
  await assert.rejects(downloadOfficial(async () => { attempts += 1; return new Response('', { status: 503 }); }), /503/);
  assert.equal(attempts, 2);
});

test('building records source provenance and does not replace a newer official check with a later HEAD', async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'gtfs-build-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const zip = path.join(dir, 'feed.zip');
  const metadata = path.join(dir, 'metadata.json');
  const output = path.join(dir, 'index.json.gz');
  const sourceRemote = { sha256: crypto.createHash('sha256').update(ZIP).digest('hex'), etag: 'download-etag', downloadedAt: '2026-11-22T17:00:00Z' };
  fs.writeFileSync(zip, ZIP);
  fs.writeFileSync(metadata, JSON.stringify({ feed: { feedVersion: 'S1000528' }, remote: sourceRemote }));
  await execFile(process.execPath, ['tools/build_gtfs_static_index.js', zip, output, metadata]);
  const payload = JSON.parse(zlib.gunzipSync(fs.readFileSync(output)));
  const index = indexMeta(payload);
  assert.equal(index.tripCount, 1);
  assert.equal(payload.trips[0][5], '24-01');
  assert.deepEqual(index.sourceRemote, sourceRemote);
  const current = { enabled: true, schedule: DEFAULT_SCHEDULE, lastCheckedAt: '2026-11-22T18:00:00Z',
    remote: { etag: 'even-newer' }, official: { feedVersion: 'S1000529' } };
  const control = builtControl(current, index);
  assert.equal(control.official.feedVersion, 'S1000529');
  assert.equal(control.index.feedVersion, 'S1000528');
  assert.equal(control.index.sourceRemote.etag, 'download-etag');
  assert.equal(control.lastRebuildRequestStatus, 'index-built');
  fs.writeFileSync(metadata, JSON.stringify({ feed: { feedVersion: 'S1000528' }, remote: { sha256: 'wrong' } }));
  await assert.rejects(execFile(process.execPath, ['tools/build_gtfs_static_index.js', zip, output, metadata]), /fingerprint/);
});

test('the production daily schedule and workflow preserve provenance without a post-build HEAD', () => {
  const control = require('../data/gtfs_static_control.json');
  assert.equal(control.enabled, true);
  assert.deepEqual(control.schedule, DEFAULT_SCHEDULE);
  const workflow = fs.readFileSync('.github/workflows/gtfs-static-index.yml', 'utf8');
  assert.match(workflow, /download_gtfs_static.js/);
  assert.match(workflow, /build_gtfs_static_index.js.*GTFSExport.meta.json/);
  assert.doesNotMatch(fs.readFileSync('tools/update_gtfs_static_control.js', 'utf8'), /fetch\(|HEAD/);
});
