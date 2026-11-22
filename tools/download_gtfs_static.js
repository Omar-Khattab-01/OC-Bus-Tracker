'use strict';

const fs = require('node:fs');
const zlib = require('node:zlib');
const { downloadOfficial, indexMeta } = require('../lib/gtfs_static_control');

async function main() {
  const [zipFile, metadataFile] = process.argv.slice(2);
  if (!zipFile || !metadataFile) throw new Error('Usage: node tools/download_gtfs_static.js output.zip metadata.json');
  const result = await downloadOfficial();
  const current = indexMeta(JSON.parse(zlib.gunzipSync(fs.readFileSync('data/gtfs_static_index.json.gz'))));
  const oldVersion = current.feedVersion.match(/^S(\d+)$/);
  const newVersion = result.feed.feedVersion.match(/^S(\d+)$/);
  if ((oldVersion && newVersion && Number(newVersion[1]) < Number(oldVersion[1])) || result.feed.feedEndDate < current.feedEndDate) {
    throw new Error(`OC returned an older export (${result.feed.feedVersion}); refusing to replace ${current.feedVersion}.`);
  }
  const metadata = { feed: result.feed, remote: { ...result.remote, downloadedAt: new Date().toISOString() } };
  fs.writeFileSync(zipFile, result.buffer);
  fs.writeFileSync(metadataFile, JSON.stringify(metadata));
  console.log(JSON.stringify({ downloaded: metadata.feed, source: metadata.remote }, null, 2));
}

main().catch((error) => { console.error(error.message); process.exitCode = 1; });
