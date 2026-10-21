'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const Stripe = require('stripe');
const { createSupportRouter, createSupportService, supporterStatus } = require('../lib/supporters');

const now = Date.parse('2026-10-20T12:00:00Z');
const future = new Date(now + 86400000).toISOString();

test('monthly recognition takes precedence and ends at the paid period boundary', () => {
  const subscription = { status: 'active', current_period_end: future, cancel_at_period_end: true };
  const payment = { amount: 500, paid_at: new Date(now).toISOString() };
  assert.equal(supporterStatus([subscription], [payment], now).kind, 'monthly');
  assert.equal(supporterStatus([subscription], [], now).cancelAtPeriodEnd, true);
  assert.equal(supporterStatus([subscription], [], Date.parse(future)).kind, 'none');
  for (const status of ['canceled', 'past_due', 'unpaid', 'incomplete', 'trialing']) {
    assert.equal(supporterStatus([{ ...subscription, status }], [], now).suppressPrompts, false);
  }
});

test('supporting-since uses the earliest verified subscription payment, not a renewal or one-time gift', () => {
  const first = '2026-08-10T12:00:00Z';
  const subs = [
    { status: 'active', current_period_end: future, first_paid_at: '2026-10-01T12:00:00Z' },
    { status: 'canceled', first_paid_at: first },
  ];
  assert.equal(supporterStatus(subs, [], now).supportingSince, new Date(first).toISOString());
  assert.equal(supporterStatus([{ status: 'active', current_period_end: future }], [], now).supportingSince, null);
});

test('one-time recognition lasts exactly 30 days, excludes refunds, and extends on another contribution', () => {
  const payment = { amount: 1000, paid_at: new Date(now).toISOString(), refunded: false };
  assert.equal(supporterStatus([], [payment], now).kind, 'one-time');
  assert.equal(Date.parse(supporterStatus([], [payment], now).expiresAt), now + 30 * 86400000);
  assert.equal(supporterStatus([], [payment], now + 30 * 86400000).kind, 'none');
  assert.equal(supporterStatus([], [{ ...payment, refunded: true }], now).kind, 'none');
  assert.equal(supporterStatus([], [{ ...payment, amount: 0 }], now).kind, 'none');
  assert.equal(Date.parse(supporterStatus([], [payment, { ...payment, paid_at: future }], now).expiresAt), Date.parse(future) + 30 * 86400000);
});

function memoryDb() {
  const tables = {};
  const keys = { support_devices: ['device_hash'], support_customers: ['id'],
    support_subscriptions: ['id'], support_payments: ['session_id'], support_device_customers: ['device_hash', 'customer_id'] };
  return {
    tables,
    auth: { getUser: async (token) => ['verified', 'owner'].includes(token) ? { data: { user: {
      email: token === 'owner' ? 'omar.hosam2000@gmail.com' : 'SUPPORTER@example.com', email_confirmed_at: future } } }
      : { data: {}, error: new Error('Invalid token') } },
    from(table) {
      tables[table] ||= [];
      let action = 'select', values, single = false;
      const filters = [];
      const query = {
        select() { return query; },
        eq(key, value) { filters.push((row) => row[key] === value); return query; },
        in(key, values) { filters.push((row) => values.includes(row[key])); return query; },
        maybeSingle() { single = true; return query; },
        upsert(value) { action = 'upsert'; values = value; return query; },
        update(value) { action = 'update'; values = value; return query; },
        then(resolve) {
          if (action === 'upsert') {
            const index = tables[table].findIndex((row) => keys[table].every((key) => row[key] === values[key]));
            if (index < 0) tables[table].push({ ...values });
            else tables[table][index] = { ...tables[table][index], ...values };
          }
          const rows = tables[table].filter((row) => filters.every((filter) => filter(row)));
          if (action === 'update') rows.forEach((row) => Object.assign(row, values));
          return Promise.resolve({ data: single ? rows[0] || null : rows, error: null }).then(resolve);
        },
      };
      return query;
    },
  };
}

function fixture() {
  const db = memoryDb();
  const stripeSdk = new Stripe('sk_test_fixture');
  const session = { id: 'cs_1', mode: 'payment', status: 'complete', payment_status: 'paid', payment_link: 'plink_support',
    payment_intent: 'pi_1', amount_total: 500, currency: 'cad', created: Math.floor(Date.now() / 1000),
    customer_details: { email: 'supporter@example.com' }, client_reference_id: 'a'.repeat(64) };
  const sub = { id: 'sub_1', customer: 'cus_1', status: 'active', cancel_at_period_end: false,
    items: { data: [{ current_period_end: Math.floor(Date.now() / 1000) + 86400 }] } };
  const stripe = {
    webhooks: stripeSdk.webhooks,
    checkout: { sessions: { retrieve: async () => session, list: async () => ({ data: [session] }) } },
    paymentLinks: { retrieve: async (id) => ({ url: id === 'plink_support' ? 'https://buy.stripe.com/7sYbJ109h33c1h15Eo0VO01' : 'https://buy.stripe.com/unrelated' }) },
    customers: { retrieve: async () => ({ id: 'cus_1', email: 'supporter@example.com' }) },
    subscriptions: { retrieve: async () => sub },
    invoices: { retrieve: async () => ({ status_transitions: { paid_at: Math.floor(now / 1000) } }) },
    paymentIntents: { retrieve: async () => ({ latest_charge: { refunded: false } }) },
    charges: { retrieve: async () => ({ payment_intent: 'pi_1', refunded: true }) },
  };
  return { db, stripe, session, sub };
}

test('verified checkout is idempotent, excludes unrelated payments, and refunds revoke recognition', async () => {
  const { db, stripe, session } = fixture();
  const service = createSupportService({ db, stripe });
  await service.syncCheckout(session.id);
  await service.syncCheckout(session.id);
  assert.equal(db.tables.support_payments.length, 1);
  assert.equal((await service.status('a'.repeat(64))).kind, 'one-time');
  assert.equal((await service.status('b'.repeat(64))).kind, 'none');
  assert.equal((await service.status('b'.repeat(64), 'supporter@example.com')).kind, 'one-time');
  assert.equal((await service.status('b'.repeat(64))).kind, 'one-time');
  session.id = 'cs_other'; session.payment_link = 'plink_other';
  await service.syncCheckout(session.id);
  assert.equal(db.tables.support_payments.length, 1);
  await service.syncEvent({ type: 'charge.refunded', data: { object: { id: 'ch_1' } } });
  assert.equal((await service.status('a'.repeat(64))).kind, 'none');
});

test('subscription notifications read current state and recognize a paid anonymous browser', async () => {
  const { db, stripe, session, sub } = fixture();
  session.mode = 'subscription'; session.customer = 'cus_1'; session.subscription = 'sub_1'; session.invoice = 'in_1';
  const service = createSupportService({ db, stripe });
  await service.syncCheckout(session.id);
  assert.equal((await service.status('a'.repeat(64))).kind, 'monthly');
  assert.equal((await service.status('a'.repeat(64))).supportingSince, new Date(now).toISOString());
  sub.status = 'canceled';
  await service.syncEvent({ type: 'customer.subscription.updated', data: { object: { id: 'sub_1', status: 'active' } } });
  assert.equal((await service.status('a'.repeat(64))).kind, 'none');
});

test('HTTP webhook validates the original signature; browser status does not expose billing details', async (t) => {
  const { db, stripe, session } = fixture();
  const app = express();
  const secret = 'whsec_fixture';
  app.use('/api/support', createSupportRouter({ db, stripe, env: {
    STRIPE_SECRET_KEY: 'sk_test_fixture', STRIPE_WEBHOOK_SECRET: secret,
  } }));
  app.use(express.json());
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  t.after(() => server.close());
  const base = `http://127.0.0.1:${server.address().port}/api/support`;
  const payload = JSON.stringify({ id: 'evt_1', livemode: false, type: 'checkout.session.completed', data: { object: { id: session.id } } });
  const signature = stripe.webhooks.generateTestHeaderString({ payload, secret });
  const sendEvent = (body, sig) => fetch(`${base}/webhook`, { method: 'POST', body, headers: { 'content-type': 'application/json', 'stripe-signature': sig } });
  assert.equal((await sendEvent(payload, signature)).status, 200);
  assert.equal((await sendEvent(payload + ' ', signature)).status, 400);
  const response = await fetch(`${base}/status`);
  assert.match(response.headers.get('set-cookie'), /HttpOnly; SameSite=Lax/);
  const cookie = response.headers.get('set-cookie').split(';')[0];
  assert.equal((await response.json()).kind, 'none');
  const recovered = await fetch(`${base}/status`, { headers: { Cookie: cookie, Authorization: 'Bearer verified' } });
  const recoveredBody = await recovered.json();
  assert.equal(recoveredBody.kind, 'one-time');
  assert.equal(recoveredBody.email, undefined);
  assert.equal(recoveredBody.customer_id, undefined);
  assert.equal((await fetch(`${base}/status`, { headers: { Authorization: 'Bearer forged' } })).status, 401);
  assert.match(recoveredBody.portalUrl, /^https:\/\/billing\.stripe\.com\/p\/login\//);
  const checkout = await fetch(`${base}/checkout?option=monthly`, { redirect: 'manual', headers: { Cookie: cookie } });
  assert.equal(checkout.status, 303);
  const checkoutUrl = new URL(checkout.headers.get('location'));
  assert.equal(checkoutUrl.origin, 'https://buy.stripe.com');
  assert.equal(checkoutUrl.searchParams.get('client_reference_id'), recoveredBody.checkoutReference);
  assert.equal((await fetch(`${base}/checkout?option=https://evil.example`, { redirect: 'manual' })).status, 400);
  for (const token of ['', 'forged', 'verified']) {
    assert.equal((await fetch(`${base}/sync`, { method: 'POST', headers: { Authorization: `Bearer ${token}` } })).status, 403);
  }
  const imported = await fetch(`${base}/sync`, { method: 'POST', headers: { Authorization: 'Bearer owner' } });
  assert.equal(imported.status, 200);
  assert.deepEqual(await imported.json(), { checked: 1, nextCursor: null });
  assert.equal(db.tables.support_payments.length, 1);
});
