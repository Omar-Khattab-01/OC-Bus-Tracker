'use strict';

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const { indexMeta, builtControl } = require('../lib/gtfs_static_control');

const root = path.resolve(__dirname, '..');
const indexFile = path.join(root, 'data', 'gtfs_static_index.json.gz');
const controlFile = path.join(root, 'data', 'gtfs_static_control.json');

function main() {
  const index = indexMeta(JSON.parse(zlib.gunzipSync(fs.readFileSync(indexFile))));
  const current = JSON.parse(fs.readFileSync(controlFile, 'utf8'));
  const control = builtControl(current, index);
  fs.writeFileSync(controlFile, JSON.stringify(control, null, 2) + '\n');
  console.log(JSON.stringify({ ok: true, builtVersion: index.feedVersion, status: 'index-built' }));
}

main();
