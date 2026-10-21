'use strict';

const fs = require('node:fs');
const path = require('node:path');
const Stripe = require('stripe');
const { createClient } = require('@supabase/supabase-js');
const { createSupportService } = require('../lib/supporters');

if (typeof process.loadEnvFile === 'function' && fs.existsSync(path.join(__dirname, '..', '.env.local'))) {
  process.loadEnvFile(path.join(__dirname, '..', '.env.local'));
}

async function main() {
  for (const key of ['SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY', 'STRIPE_SECRET_KEY']) {
    if (!process.env[key]) throw new Error(`Missing ${key}`);
  }
  const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, { maxNetworkRetries: 2 });
  const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  const service = createSupportService({ db, stripe });
  let checked = 0;
  // Re-runnable: checkout/session IDs are unique, and subscriptions are read in their current state.
  for await (const session of stripe.checkout.sessions.list({ status: 'complete', limit: 100 })) {
    await service.syncCheckout(session.id);
    checked += 1;
  }
  console.log(`Finished checking ${checked} completed checkout sessions. Only support-link payments were imported.`);
}

main().catch((error) => { console.error(error.message); process.exitCode = 1; });
