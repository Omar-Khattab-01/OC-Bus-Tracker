'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const root = path.resolve(__dirname, '..');
const indexFile = path.join(root, 'data', 'gtfs_static_index.json.gz');
const controlFile = path.join(root, 'data', 'gtfs_static_control.json');
const officialUrl = 'https://oct-gtfs-emasagcnfmcgeham.z01.azurefd.net/public-access/GTFSExport.zip';

function readJsonFile(filePath, fallback = {}) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (_) {
    return fallback;
  }
}

function readIndexMeta() {
  const payload = JSON.parse(zlib.gunzipSync(fs.readFileSync(indexFile)).toString('utf8'));
  const feedInfo = payload?.meta?.feedInfo || {};
  return {
    generatedAt: String(payload?.meta?.generatedAt || ''),
    feedVersion: String(feedInfo.feed_version || ''),
    feedStartDate: String(feedInfo.feed_start_date || ''),
    feedEndDate: String(feedInfo.feed_end_date || ''),
    tripCount: Number(payload?.meta?.tripCount || payload?.trips?.length || 0),
    stopCount: Number(payload?.meta?.stopCount || payload?.stops?.length || 0),
  };
}

async function headOfficialGtfs() {
  const response = await fetch(officialUrl, { method: 'HEAD' });
  if (!response.ok) {
    throw new Error(`Official GTFS HEAD failed with HTTP ${response.status}.`);
  }
  return {
    url: officialUrl,
    lastModified: response.headers.get('last-modified') || '',
    etag: response.headers.get('etag') || '',
    contentLength: Number(response.headers.get('content-length') || 0),
  };
}

async function main() {
  const now = new Date().toISOString();
  const current = readJsonFile(controlFile, {});
  const remote = await headOfficialGtfs();
  const control = {
    enabled: Boolean(current.enabled),
    timezone: 'America/Toronto',
    schedule: Array.isArray(current.schedule) ? current.schedule : [],
    lastCheckedAt: now,
    lastScheduledCheckKey: String(current.lastScheduledCheckKey || ''),
    lastChangeDetectedAt: String(current.lastChangeDetectedAt || ''),
    lastRebuildRequestedAt: String(current.lastRebuildRequestedAt || ''),
    lastRebuildRequestStatus: 'index-current',
    remote,
    index: readIndexMeta(),
  };

  fs.writeFileSync(controlFile, `${JSON.stringify(control, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, control }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
