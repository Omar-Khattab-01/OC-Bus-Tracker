'use strict';

module.exports = (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.statusCode = 410;
  const page = new URL(req.url, 'http://localhost').searchParams.get('view') === 'page';
  if (page) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Holiday paddles retired</title><h1>These holiday paddles are no longer available</h1><p>Please use the current paddle lookup.</p><a href="/">Back to Lookup</a></html>');
    return;
  }
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify({ ok: false, error: 'These seasonal holiday paddles have been retired. Please use the current paddle lookup.' }));
};
