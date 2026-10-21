'use strict';

// Isolated visual fixtures. This server is loopback-only and must never be deployed.
const express = require('express');
const app = express();
const port = Number(process.env.SUPPORT_PREVIEW_PORT || 7863);

app.get('/preview/:kind', (req, res) => {
  if (!['monthly', 'one-time', 'none'].includes(req.params.kind)) return res.sendStatus(404);
  res.cookie('oc_support_preview', req.params.kind, { httpOnly: true, sameSite: 'lax' });
  res.redirect('/');
});
app.get('/api/support/status', (req, res) => {
  const kind = String(req.headers.cookie || '').match(/(?:^|;\s*)oc_support_preview=(monthly|one-time|none)(?:;|$)/)?.[1] || 'none';
  res.set('Cache-Control', 'no-store').json({ enabled: true, kind, suppressPrompts: kind !== 'none',
    supportingSince: kind === 'monthly' ? '2026-10-01T16:00:00Z' : null,
    expiresAt: kind !== 'none' ? new Date(Date.now() + 30 * 86400000).toISOString() : null,
    portalUrl: 'https://billing.stripe.com/p/login/6oU28r6xFavE7Fp3wg0VO00' });
});
app.use(require('../server'));
app.listen(port, '127.0.0.1', () => {
  console.log(`Visual fixtures only: http://localhost:${port}/preview/monthly`);
  console.log(`One-time fixture: http://localhost:${port}/preview/one-time`);
});
