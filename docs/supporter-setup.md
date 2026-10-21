# Supporter Recognition and Subscription Management

The customer portal is active in the Ottawa Operators Tools Stripe account:
https://billing.stripe.com/p/login/6oU28r6xFavE7Fp3wg0VO00

Cancellation is enabled at the end of the billing period. The public support
page links directly to this portal, which verifies the customer's payment email.
This works independently of website accounts and supporter recognition.

## Required Server Configuration

Configure these variables in the existing project's environment, never in public
JavaScript or version control:

- `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`: the existing
  OC site's Supabase project (not another project's database).
- `STRIPE_SECRET_KEY`: the existing support account's API key, with permission to
  read Checkout Sessions, Payment Links, Customers, Subscriptions, Payment Intents,
  and Charges. This integration does not need to create payments or cancel
  subscriptions through the API; Stripe's portal handles those customer actions.
- `STRIPE_WEBHOOK_SECRET`: signing secret for this site's webhook endpoint.
- `STRIPE_CUSTOMER_PORTAL_URL`: optional override of the active portal link above.

Local credentials belong in the ignored `.env.local`. Production keys must remain
in the deployment environment. The live portal must be paired with live keys;
use separate test-mode links and keys for payment testing.

## Database

Apply `supabase/migration_supporters.sql` to the existing OC database. The five
tables have RLS enabled and no browser-client access. Only the service role can
read and write billing records.

Also apply `supabase/migration_supporter_since.sql` before releasing the banner
updates. This stores the first subscription invoice's actual paid timestamp;
renewals preserve it. Re-run the import to backfill existing subscriptions.
The monthly banner omits the date when it has not been verified.

The thank-you banner can be dismissed independently of supporter status. That
preference lasts 30 days in the same browser for the current supporter type,
across pages and tabs. The banner returns only if recognition is still active.
A monthly upgrade shows its new banner. Supporters can restore
a dismissed banner from **Already a supporter?** on the support page.

## Stripe Webhook

Register `https://YOUR_OC_SITE/api/support/webhook` in Stripe Workbench and enable:

- `checkout.session.completed`
- `checkout.session.async_payment_succeeded`
- `customer.subscription.created`
- `customer.subscription.updated`
- `customer.subscription.deleted`
- `invoice.paid`
- `invoice.payment_failed`
- `customer.updated`
- `charge.refunded`
- `charge.dispute.created`
- `charge.dispute.closed`

The endpoint must receive the original raw JSON body. Express mounts this route
before its global JSON parser. Confirm a valid signed delivery reaches the Vercel
catch-all handler and returns 200 before enabling public recognition.
Invalid signatures or mismatched live/test modes are rejected.

Only the six existing support Payment Links in `lib/supporters.js` qualify.
New links, replacement links, or test-mode links must be added there explicitly.

## Recognizing Supporters

Checkout opens through `/api/support/checkout?option=monthly` (or a one-time
option). The server creates a random HttpOnly cookie and sends only its SHA-256
reference to Stripe. A signed, successful payment notification connects the
contribution to that browser. A click alone never grants supporter status.

Monthly recognition lasts while Stripe reports an active subscription with a
future paid-period end. Scheduled cancellation keeps recognition until that end.
One-time recognition lasts 30 days after the successful charge. Full refunds
and disputes revoke one-time recognition. Renewals and status changes arrive
through webhooks. Duplicate deliveries use unique Stripe IDs and are re-runnable.

Every page using `oc-header.js` checks the same server status. Both recognized
supporter types get distinct personal thank-you banners, and fundraising prompts
are hidden. Banner status is not public and is never a paid feature gate.

## Email Recovery and Existing Accounts

On the support page, a supporter can verify their payment email using Supabase's
existing passwordless email flow. It uses the same Supabase identity/session as
the site's accounts; first-time verification creates an identity, without asking
the supporter to choose a password. Existing signup restrictions still apply.

Add `https://YOUR_OC_SITE/support?restore=1` to Supabase Auth's allowed redirect
URLs, configure the site's canonical URL, and confirm email delivery/SMTP.
For local testing, allow the local support callback separately. Do not loosen
production redirect rules to arbitrary origins.

The backend validates the confirmed email through Supabase `getUser` before
linking it to billing records. A typed email, localStorage flag, or payment-return
URL alone cannot grant recognition. This restores recognition after clearing
cookies or switching devices. The Stripe portal has its own email verification
and remains available even if website email recovery is unavailable.

## Existing Contributions

After configuring the database and the matching Stripe account, run:

```sh
node tools/sync_supporters.js
```

This imports completed checkouts from the six support links and reads the current
subscription/refund state. Re-running is safe. It logs counts, not customer emails
or payment details. Historical visitors without a checkout browser reference must
verify their payment email once. Contributions older than 30 days remain recorded
but do not grant a new month of one-time recognition.

Alternatively, verify the owner's email on the support page and select **Import
existing contributions**. This owner-only operation imports five sessions per
request to stay within function time limits. The API validates the confirmed
owner email on every request. It is also safe to re-run after interruption.

## Verification

```sh
npm test
node tools/preview_supporters.js
```

The loopback-only visual fixture server provides `/preview/monthly`,
`/preview/one-time`, and `/preview/none` on port 7863. These are synthetic UI
fixtures, not real payments or a production feature. Normal local development
uses the real app on a different port.

Before publishing, verify a test payment, its signed webhook, database record,
browser recognition, email recovery, and a portal cancellation. Confirm renewing,
refunding, and end-of-period cancellation update recognition as expected.
