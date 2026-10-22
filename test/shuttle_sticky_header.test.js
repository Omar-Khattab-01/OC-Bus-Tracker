'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

for (const file of ['shuttles.html', 'index.html']) {
  test(file + ' shuttle stop headers stick to the schedule scroll container', () => {
    const html = fs.readFileSync(path.join(__dirname, '..', 'public', file), 'utf8');
    const table = html.match(/\.schedule-table\s*\{([^}]+)\}/)[1];
    const header = html.match(/\.schedule-table th\s*\{([^}]+)\}/)[1];
    assert.match(table, /overflow:\s*visible/);
    assert.match(table, /border-collapse:\s*separate/);
    assert.match(header, /position:\s*sticky/);
    assert.match(header, /top:\s*0/);
    assert.match(header, /z-index:\s*2/);
    assert.match(header, /background:\s*#[0-9a-f]{6}/i);
  });
}
