'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const script = fs.readFileSync(path.join(__dirname, '..', 'public', 'support-status.js'), 'utf8');
const DAY = 86400000;

async function openPage({ now, storage, status }) {
  const nodes = new Map();
  const classes = new Set();
  function element() {
    return { children: [], dataset: {}, handlers: {},
      setAttribute() {}, addEventListener(event, handler) { this.handlers[event] = handler; },
      appendChild(node) { this.children.push(node); if (node.id) nodes.set(node.id, node); },
      replaceChildren(...children) { this.children = children; },
    };
  }
  const target = element();
  const document = { documentElement: { classList: { toggle(name, value) {
    if (value) classes.add(name); else classes.delete(name);
  } } }, body: target, visibilityState: 'hidden', addEventListener() {},
  getElementById: (id) => nodes.get(id), querySelector: () => target, createElement: element };
  class ClockDate extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return now; }
  }
  const window = { addEventListener() {}, dispatchEvent() {} };
  const context = vm.createContext({ window, document, Date: ClockDate, Intl, AbortSignal,
    location: { protocol: 'http:' },
    localStorage: { getItem: (key) => storage.get(key) || null,
      setItem: (key, value) => storage.set(key, value), removeItem: (key) => storage.delete(key) },
    fetch: async () => ({ ok: true, json: async () => ({ ...status }) }),
    setTimeout: () => 1, clearTimeout() {}, CustomEvent: class { constructor(type, options) { Object.assign(this, { type }, options); } },
  });
  vm.runInContext(script, context);
  await window.OCSupport.ready;
  return { banner: nodes.get('ocSupporterBanner'), support: window.OCSupport, classes };
}

test('banner dismissal lasts exactly 30 days across page loads without removing supporter recognition', async () => {
  const now = Date.parse('2026-10-20T12:00:00Z');
  const storage = new Map();
  const status = { kind: 'monthly', suppressPrompts: true, expiresAt: new Date(now + 90 * DAY).toISOString() };
  let page = await openPage({ now, storage, status });
  assert.equal(page.banner.hidden, false);
  page.banner.children.at(-1).handlers.click();
  assert.equal(page.banner.hidden, true);
  assert.equal(page.support.status.suppressPrompts, true);
  assert.equal(page.classes.has('oc-is-supporter'), true);
  page = await openPage({ now: now + 30 * DAY - 1, storage, status });
  assert.equal(page.banner.hidden, true);
  page = await openPage({ now: now + 30 * DAY, storage, status });
  assert.equal(page.banner.hidden, false);
});

test('a hidden banner can be restored early; expired recognition never shows a thank-you banner', async () => {
  const now = Date.parse('2026-10-20T12:00:00Z');
  const storage = new Map();
  const status = { kind: 'one-time', suppressPrompts: true, expiresAt: new Date(now + 10 * DAY).toISOString() };
  let page = await openPage({ now, storage, status });
  page.banner.children.at(-1).handlers.click();
  page.support.showBanner();
  assert.equal(page.banner.hidden, false);
  page.banner.children.at(-1).handlers.click();
  page = await openPage({ now: now + 31 * DAY, storage, status });
  assert.equal(page.banner.hidden, true);
  assert.equal(page.classes.has('oc-is-supporter'), false);
});
