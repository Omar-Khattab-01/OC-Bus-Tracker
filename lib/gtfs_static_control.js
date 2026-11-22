'use strict';

const crypto = require('node:crypto');
const unzipper = require('unzipper');
const { parse } = require('csv-parse/sync');

const OFFICIAL_URL = 'https://oct-gtfs-emasagcnfmcgeham.z01.azurefd.net/public-access/GTFSExport.zip';
const DAY_NAMES = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
const DEFAULT_SCHEDULE = [{ id: 'daily-0200', days: DAY_NAMES, time: '02:00' }];

function indexMeta(payload = {}) {
  const meta = payload.meta || {};
  const feed = meta.feedInfo || {};
  return {
    generatedAt: String(meta.generatedAt || ''),
    feedVersion: String(feed.feed_version || ''),
    feedStartDate: String(feed.feed_start_date || ''),
    feedEndDate: String(feed.feed_end_date || ''),
    tripCount: Number(meta.tripCount || payload.trips?.length || 0),
    stopCount: Number(meta.stopCount || payload.stops?.length || 0),
    sourceRemote: meta.sourceRemote || null,
  };
}

function sameRemote(left, right) {
  if (!left || !right) return null;
  if (left.sha256 && right.sha256) return left.sha256 === right.sha256;
  let compared = false;
  for (const key of ['etag', 'lastModified', 'contentLength']) {
    if (!left[key] || !right[key]) continue;
    compared = true;
    if (String(left[key]) !== String(right[key])) return false;
  }
  return compared ? true : null;
}

function comparison(control, deployed, now = Date.now()) {
  const official = control.official;
  if (control.lastCheckError) return { state: 'unverified', label: 'Unable to verify', message: control.lastCheckError };
  if (!official?.feedVersion || !deployed?.feedVersion) {
    return { state: 'unverified', label: 'Verification needed', message: 'Check the official feed to verify its version against the deployed index.' };
  }
  const versionDiffers = ['feedVersion', 'feedStartDate', 'feedEndDate'].some((key) => official[key] !== deployed[key]);
  const fingerprint = sameRemote(deployed.sourceRemote, official.remote);
  if (versionDiffers || fingerprint === false) {
    return { state: 'update_available', label: 'Update available', message: `Official ${official.feedVersion}; live website ${deployed.feedVersion}. The deployed index needs rebuilding.` };
  }
  if (fingerprint === null) {
    return { state: 'update_available', label: 'Rebuild needed', message: 'This index has no verified source fingerprint. Rebuild once to establish a reliable baseline.' };
  }
  if (sameRemote(official.remote, control.remote) !== true) {
    return { state: 'unverified', label: 'Verification needed', message: 'The download metadata changed after the last version verification.' };
  }
  if (!Number.isFinite(Date.parse(official.verifiedAt)) || now - Date.parse(official.verifiedAt) > 36 * 3600000) {
    return { state: 'unverified', label: 'Verification overdue', message: 'The last official version verification is more than 36 hours old.' };
  }
  return { state: 'up_to_date', label: 'Up to date', message: `Official and live website both use ${deployed.feedVersion}, with matching source fingerprints.` };
}

function ottawaParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto', weekday: 'long', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(date);
  const p = Object.fromEntries(parts.map(({ type, value }) => [type, value]));
  return { weekday: p.weekday.toLowerCase(), time: `${p.hour}:${p.minute}`, date: `${p.year}-${p.month}-${p.day}` };
}

function dueSchedule(control, date = new Date()) {
  if (!control.enabled) return null;
  if (control.lastCheckError && date.valueOf() - Date.parse(control.lastAttemptAt || 0) < 15 * 60000) return null;
  const current = ottawaParts(date);
  // Catch up after a missed cron minute, including the spring DST jump over 02:00.
  const due = (control.schedule || []).filter((row) => row.days?.includes(current.weekday) && row.time <= current.time)
    .sort((a, b) => b.time.localeCompare(a.time))[0];
  if (!due) return null;
  const key = `${current.date}T${due.time}`;
  if (String(control.lastScheduledCheckKey || '') >= key) return null;
  return { schedule: due, current: { ...current, key } };
}

function sanitizeSchedule(entries) {
  if (!Array.isArray(entries) || entries.length > 50) throw new Error('Use no more than 50 schedule rows.');
  return entries.map((entry) => {
    const time = String(entry.time || '');
    const days = [...new Set((entry.days || []).filter((day) => DAY_NAMES.includes(day)))];
    if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(time) || !days.length) throw new Error('Each schedule needs valid days and an HH:MM time.');
    return { id: String(entry.id || crypto.randomUUID()).replace(/[^\w-]/g, '').slice(0, 80), days, time };
  });
}

function remoteHeaders(response) {
  return { url: OFFICIAL_URL, etag: response.headers.get('etag') || '',
    lastModified: response.headers.get('last-modified') || '', contentLength: Number(response.headers.get('content-length') || 0) };
}

async function officialRequest(method, fetchImpl = fetch) {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const response = await fetchImpl(OFFICIAL_URL, { method, headers: { 'Cache-Control': 'no-cache', 'Accept-Encoding': 'identity' }, signal: AbortSignal.timeout(25000) });
      if (!response.ok) throw new Error(`Official GTFS ${method} returned HTTP ${response.status}.`);
      if (method === 'HEAD') return { remote: remoteHeaders(response) };
      if (Number(response.headers.get('content-length')) > 150 * 1024 * 1024) throw new Error('Official GTFS ZIP exceeds the 150 MB safety limit.');
      const chunks = [];
      let size = 0;
      for await (const chunk of response.body) {
        size += chunk.length;
        if (size > 150 * 1024 * 1024) throw new Error('Official GTFS ZIP exceeds the 150 MB safety limit.');
        chunks.push(chunk);
      }
      const buffer = Buffer.concat(chunks);
      const remote = { ...remoteHeaders(response), contentLength: buffer.length,
        sha256: crypto.createHash('sha256').update(buffer).digest('hex') };
      return { buffer, remote };
    } catch (error) {
      if (attempt === 1) throw error;
    }
  }
}

async function inspectFeed(buffer) {
  const zip = await unzipper.Open.buffer(buffer);
  const entry = zip.files.find((file) => file.path.split('/').pop() === 'feed_info.txt');
  if (!entry || entry.uncompressedSize > 1024 * 1024) throw new Error('GTFS ZIP has no valid feed_info.txt.');
  const feed = parse(await entry.buffer(), { columns: true, skip_empty_lines: true, bom: true })[0];
  if (!feed?.feed_version || !/^\d{8}$/.test(feed.feed_start_date) || !/^\d{8}$/.test(feed.feed_end_date)) {
    throw new Error('Official GTFS feed version or coverage is missing.');
  }
  return { feedVersion: feed.feed_version, feedStartDate: feed.feed_start_date, feedEndDate: feed.feed_end_date };
}

async function downloadOfficial(fetchImpl = fetch) {
  const result = await officialRequest('GET', fetchImpl);
  return { ...result, feed: await inspectFeed(result.buffer) };
}

function builtControl(current, index, now = new Date().toISOString()) {
  if (!index.sourceRemote?.sha256) throw new Error('The built index has no verified download fingerprint.');
  const downloadedAt = index.sourceRemote.downloadedAt;
  const newerCheck = Date.parse(current.lastCheckedAt || 0) > Date.parse(downloadedAt);
  return {
    ...current,
    lastCheckedAt: newerCheck ? current.lastCheckedAt : downloadedAt,
    remote: newerCheck ? current.remote : index.sourceRemote,
    official: newerCheck && current.official ? current.official : {
      feedVersion: index.feedVersion, feedStartDate: index.feedStartDate, feedEndDate: index.feedEndDate,
      verifiedAt: downloadedAt, remote: index.sourceRemote,
    },
    lastRebuildRequestStatus: 'index-built', lastRebuildError: '', lastBuiltAt: now,
    lastCheckError: newerCheck ? current.lastCheckError || '' : '',
    index,
  };
}

module.exports = { OFFICIAL_URL, DEFAULT_SCHEDULE, indexMeta, sameRemote, comparison,
  ottawaParts, dueSchedule, sanitizeSchedule, officialRequest, inspectFeed, downloadOfficial, builtControl };
