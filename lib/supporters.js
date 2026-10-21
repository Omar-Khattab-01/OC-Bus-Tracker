'use strict';

const crypto = require('node:crypto');
const express = require('express');
const Stripe = require('stripe');

const DAY = 86400000;
const COOKIE = 'oc_supporter';
const DEFAULT_PORTAL_URL = 'https://billing.stripe.com/p/login/6oU28r6xFavE7Fp3wg0VO00';
const emailKey = (value) => String(value || '').trim().toLowerCase();
const objectId = (value) => typeof value === 'string' ? value : value?.id;
const SUPPORT_LINKS = {
  oneTime3: 'https://buy.stripe.com/3cIeVd4px8nw5xh5Eo0VO02',
  oneTime5: 'https://buy.stripe.com/7sYbJ109h33c1h15Eo0VO01',
  oneTime10: 'https://buy.stripe.com/6oU28r7BJ8nwcZJd6Q0VO03',
  oneTime20: 'https://buy.stripe.com/4gM5kDaNVgU26Blgj20VO04',
  customAmount: 'https://buy.stripe.com/4gM00j09h33cf7RaYI0VO05',
  monthly: 'https://buy.stripe.com/6oU28r6xFavE7Fp3wg0VO00',
};
const SUPPORT_LINK_URLS = new Set(Object.values(SUPPORT_LINKS));

function supporterStatus(subscriptions, payments, now = Date.now()) {
  const monthly = subscriptions.filter((sub) => sub.status === 'active'
    && Date.parse(sub.current_period_end) > now);
  if (monthly.length) {
    const firstPayments = subscriptions.map((sub) => Date.parse(sub.first_paid_at)).filter((date) => Number.isFinite(date) && date <= now);
    return { kind: 'monthly', expiresAt: new Date(Math.max(...monthly.map((sub) => Date.parse(sub.current_period_end)))).toISOString(),
      supportingSince: firstPayments.length ? new Date(Math.min(...firstPayments)).toISOString() : null,
      cancelAtPeriodEnd: monthly.every((sub) => sub.cancel_at_period_end), suppressPrompts: true };
  }
  const expires = payments.filter((payment) => !payment.refunded && payment.amount > 0)
    .map((payment) => Date.parse(payment.paid_at) + 30 * DAY).filter(Number.isFinite);
  const until = Math.max(0, ...expires);
  return { kind: until > now ? 'one-time' : 'none', expiresAt: until > now ? new Date(until).toISOString() : null,
    cancelAtPeriodEnd: false, suppressPrompts: until > now };
}

async function dbResult(query) {
  const { data, error } = await query;
  if (error) throw error;
  return data;
}

function createSupportService({ db, stripe }) {
  async function isSupportCheckout(session) {
    if (!session.payment_link) return false;
    const link = await stripe.paymentLinks.retrieve(objectId(session.payment_link));
    return SUPPORT_LINK_URLS.has(link.url);
  }

  async function syncCustomer(customerId) {
    if (!customerId) return null;
    const customer = await stripe.customers.retrieve(customerId);
    await dbResult(db.from('support_customers').upsert({ id: customer.id, email: emailKey(customer.email) || null,
      updated_at: new Date().toISOString() }));
    return customer;
  }

  async function syncSubscription(subscriptionId, trustedCheckout = null) {
    // Fetch current state so delayed Stripe events cannot restore an old subscription state.
    const sub = await stripe.subscriptions.retrieve(subscriptionId);
    const known = await dbResult(db.from('support_subscriptions').select('id,first_paid_at').eq('id', sub.id).maybeSingle());
    let checkout = trustedCheckout;
    if (!checkout && (!known || !known.first_paid_at)) {
      const sessions = await stripe.checkout.sessions.list({ subscription: sub.id, limit: 1 });
      checkout = sessions.data[0];
    }
    if (!known && !trustedCheckout && (!checkout || !await isSupportCheckout(checkout))) return;
    let firstPaidAt = known?.first_paid_at || null;
    if (!firstPaidAt && checkout?.payment_status === 'paid' && checkout.invoice) {
      const invoice = await stripe.invoices.retrieve(objectId(checkout.invoice));
      if (invoice.status_transitions?.paid_at) firstPaidAt = new Date(invoice.status_transitions.paid_at * 1000).toISOString();
    }
    await syncCustomer(objectId(sub.customer));
    const periodEnds = (sub.items?.data || []).map((item) => item.current_period_end).filter(Number.isFinite);
    const periodEnd = sub.current_period_end || Math.max(0, ...periodEnds);
    await dbResult(db.from('support_subscriptions').upsert({
      id: sub.id, customer_id: objectId(sub.customer), status: sub.status,
      current_period_end: periodEnd ? new Date(periodEnd * 1000).toISOString() : null,
      first_paid_at: firstPaidAt,
      cancel_at_period_end: Boolean(sub.cancel_at_period_end), updated_at: new Date().toISOString(),
    }));
  }

  async function syncCheckout(sessionId) {
    const session = await stripe.checkout.sessions.retrieve(sessionId);
    if (session.status !== 'complete' || session.payment_status !== 'paid' || !await isSupportCheckout(session)) return;
    const customerId = objectId(session.customer);
    const customer = await syncCustomer(customerId);
    const deviceHash = /^[a-f0-9]{64}$/.test(session.client_reference_id || '') ? session.client_reference_id : null;
    if (customerId && deviceHash) {
      await dbResult(db.from('support_device_customers').upsert({ device_hash: deviceHash, customer_id: customerId }));
    }
    if (session.mode === 'subscription') {
      await syncSubscription(objectId(session.subscription), session);
    } else if (session.mode === 'payment' && session.amount_total > 0) {
      const paymentIntentId = objectId(session.payment_intent);
      // Read refunds again on retry instead of overwriting a previously refunded payment as paid.
      const intent = paymentIntentId ? await stripe.paymentIntents.retrieve(paymentIntentId, { expand: ['latest_charge'] }) : null;
      await dbResult(db.from('support_payments').upsert({
        session_id: session.id, payment_intent_id: paymentIntentId || null, customer_id: customerId || null,
        device_hash: deviceHash, email: emailKey(session.customer_details?.email || customer?.email) || null,
        amount: session.amount_total, currency: session.currency,
        paid_at: new Date((intent?.latest_charge?.created || session.created) * 1000).toISOString(),
        refunded: Boolean(intent?.latest_charge?.refunded || intent?.latest_charge?.disputed),
      }));
    }
  }

  async function syncEvent(event) {
    const object = event.data.object;
    if (['checkout.session.completed', 'checkout.session.async_payment_succeeded'].includes(event.type)) {
      await syncCheckout(object.id);
    } else if (event.type.startsWith('customer.subscription.')) {
      await syncSubscription(object.id);
    } else if (['invoice.paid', 'invoice.payment_failed'].includes(event.type)) {
      const subscriptionId = objectId(object.subscription || object.parent?.subscription_details?.subscription);
      if (subscriptionId) await syncSubscription(subscriptionId);
    } else if (event.type === 'customer.updated') {
      const known = await dbResult(db.from('support_customers').select('id').eq('id', object.id).maybeSingle());
      if (known) await syncCustomer(object.id);
    } else if (['charge.refunded', 'charge.dispute.created', 'charge.dispute.closed'].includes(event.type)) {
      const charge = await stripe.charges.retrieve(event.type === 'charge.refunded' ? object.id : objectId(object.charge));
      await dbResult(db.from('support_payments').update({ refunded: Boolean(charge.refunded || charge.disputed) })
        .eq('payment_intent_id', objectId(charge.payment_intent)));
    }
  }

  async function status(deviceHash, verifiedEmail) {
    if (verifiedEmail) {
      await dbResult(db.from('support_devices').upsert({ device_hash: deviceHash, verified_email: verifiedEmail,
        updated_at: new Date().toISOString() }));
    }
    const device = await dbResult(db.from('support_devices').select('verified_email').eq('device_hash', deviceHash).maybeSingle());
    const email = verifiedEmail || device?.verified_email;
    const links = await dbResult(db.from('support_device_customers').select('customer_id').eq('device_hash', deviceHash));
    const customers = email ? await dbResult(db.from('support_customers').select('id').eq('email', email)) : [];
    const ids = [...new Set([...links.map((link) => link.customer_id), ...customers.map((customer) => customer.id)])];
    const subscriptions = ids.length
      ? await dbResult(db.from('support_subscriptions').select('*').in('customer_id', ids)) : [];
    const devicePayments = await dbResult(db.from('support_payments').select('amount,paid_at,refunded').eq('device_hash', deviceHash));
    const emailPayments = email ? await dbResult(db.from('support_payments').select('amount,paid_at,refunded').eq('email', email)) : [];
    return supporterStatus(subscriptions, [...devicePayments, ...emailPayments]);
  }

  async function syncHistoryPage(cursor) {
    const page = await stripe.checkout.sessions.list({ status: 'complete', limit: 5, ...(cursor ? { starting_after: cursor } : {}) });
    for (const session of page.data) await syncCheckout(session.id);
    return { checked: page.data.length, nextCursor: page.has_more ? page.data.at(-1)?.id : null };
  }

  return { syncEvent, syncCheckout, syncSubscription, syncHistoryPage, status };
}

function createSupportRouter({ db, env = process.env, stripe: stripeOverride } = {}) {
  const router = express.Router();
  const stripe = stripeOverride || (env.STRIPE_SECRET_KEY ? new Stripe(env.STRIPE_SECRET_KEY, { maxNetworkRetries: 2, timeout: 10000 }) : null);
  const service = db && stripe ? createSupportService({ db, stripe }) : null;
  const enabled = Boolean(service && env.STRIPE_WEBHOOK_SECRET);
  const configuredPortal = env.STRIPE_CUSTOMER_PORTAL_URL || DEFAULT_PORTAL_URL;
  const adminEmail = emailKey(env.INCIDENT_FEEDBACK_ADMIN_EMAIL || 'omar.hosam2000@gmail.com');
  const portalUrl = /^https:\/\/billing\.stripe\.com\/p\/login\/(?:test_)?[a-zA-Z0-9]+$/.test(configuredPortal)
    ? configuredPortal : null;

  router.use((_req, res, next) => { res.set('Cache-Control', 'no-store'); next(); });
  router.post('/webhook', express.raw({ type: 'application/json', limit: '1mb' }), async (req, res) => {
    if (!enabled) return res.status(503).json({ error: 'Support tracking is not configured.' });
    let event;
    try {
      event = stripe.webhooks.constructEvent(req.body, req.get('stripe-signature'), env.STRIPE_WEBHOOK_SECRET);
    } catch (_) {
      return res.status(400).json({ error: 'Invalid Stripe signature.' });
    }
    if (event.livemode !== /^(sk|rk)_live_/.test(env.STRIPE_SECRET_KEY)) {
      return res.status(400).json({ error: 'Stripe mode mismatch.' });
    }
    try {
      await service.syncEvent(event);
      res.json({ received: true });
    } catch (_) {
      console.error('Support webhook processing failed', event.id, event.type);
      res.status(500).json({ error: 'Unable to process event. Please retry.' });
    }
  });

  function deviceHash(req, res) {
    const cookie = String(req.headers.cookie || '').split(';').map((part) => part.trim()).find((part) => part.startsWith(`${COOKIE}=`));
    let token = cookie?.slice(COOKIE.length + 1);
    if (!/^[a-f0-9]{64}$/.test(token || '')) {
      token = crypto.randomBytes(32).toString('hex');
      res.append('Set-Cookie', `${COOKIE}=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000${req.secure || env.VERCEL ? '; Secure' : ''}`);
    }
    return crypto.createHash('sha256').update(token).digest('hex');
  }

  router.get('/checkout', (req, res) => {
    const link = Object.hasOwn(SUPPORT_LINKS, req.query.option) ? SUPPORT_LINKS[req.query.option] : null;
    if (!link) return res.status(400).json({ error: 'Choose a valid support amount.' });
    const url = new URL(link);
    url.searchParams.set('client_reference_id', deviceHash(req, res));
    res.redirect(303, url.href);
  });

  router.get('/status', async (req, res) => {
    if (!enabled) return res.json({ enabled: false, kind: 'none', suppressPrompts: false, portalUrl });
    try {
      let email = null;
      const bearer = String(req.get('authorization') || '').match(/^Bearer (.+)$/i);
      if (bearer) {
        const { data, error } = await db.auth.getUser(bearer[1]);
        if (error || !data.user?.email_confirmed_at) return res.status(401).json({ error: 'Please verify your email again.' });
        email = emailKey(data.user.email);
      }
      const hash = deviceHash(req, res);
      const status = await service.status(hash, email);
      res.json({ enabled: true, ...status, checkoutReference: hash, portalUrl, canSync: email === adminEmail });
    } catch (_) {
      res.status(503).json({ error: 'Supporter recognition is temporarily unavailable.' });
    }
  });

  router.post('/sync', express.json({ limit: '4kb' }), async (req, res) => {
    if (!enabled) return res.status(503).json({ error: 'Support tracking is not configured.' });
    try {
      const bearer = String(req.get('authorization') || '').match(/^Bearer (.+)$/i);
      const { data, error } = bearer ? await db.auth.getUser(bearer[1]) : { error: true };
      if (error || !data.user?.email_confirmed_at || emailKey(data.user.email) !== adminEmail) {
        return res.status(403).json({ error: 'Owner access required.' });
      }
      const cursor = req.body?.cursor;
      if (cursor && (typeof cursor !== 'string' || !/^cs_[A-Za-z0-9_]{1,200}$/.test(cursor))) {
        return res.status(400).json({ error: 'Invalid import cursor.' });
      }
      res.json(await service.syncHistoryPage(cursor));
    } catch (_) {
      res.status(503).json({ error: 'Import paused. Please try again; existing records will not be duplicated.' });
    }
  });

  router.use((_req, res) => res.status(404).json({ error: 'Support endpoint not found.' }));
  return router;
}

module.exports = { createSupportRouter, createSupportService, supporterStatus };
