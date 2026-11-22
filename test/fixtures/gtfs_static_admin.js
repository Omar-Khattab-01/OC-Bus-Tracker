'use strict';

const { createGtfsStaticAdmin } = require('../../lib/gtfs_static_admin');

const ZIP = Buffer.from('UEsDBBQAAAAIAC14dl3/SQf0dAAAAJ8AAAANAAAAZmVlZF9pbmZvLnR4dFXKMQ7CMAyF4b2n4ABR61gCIVYOwED3KDQurZQ6keOS64MUBti+9+vNRMHl/RHXspA49huZ+b/tEluKnp9NRb2oC16/Z+Lws14kZU3c3a6HUTyXnMyimstlGGqtfZq01X5KmyE2CHiyFqEBP7hbADjiuXsDUEsDBBQAAAAIAC14dl3WAo75HgAAACAAAAAKAAAAcm91dGVzLnR4dCvKLy1Jjc9M0SkCM4oz8otK4vMSc1O5jEx0jEy4AFBLAwQUAAAACAAteHZdwl3jQEYAAABQAAAACQAAAHRyaXBzLnR4dCvKLy1Jjc9M0SlOLSrLTAYzS4oyC+B0RmpiSnFmep5OUk5+cjZQmMvIRMdQx8jCwsTA0EDHKScxOTuptChPx8hE18CQCwBQSwMEFAAAAAgALXh2XeC8vfUzAAAAQwAAAAkAAABzdG9wcy50eHQrLskviM9M0SkG0XmJuakQVk5iCZSRn8dlqBNcklhUomNiqqNrbsplpOOWmZdZnAHjAwBQSwMEFAAAAAgALXh2XSQchn1QAAAAdgAAAA4AAABzdG9wX3RpbWVzLnR4dCspyiyIz0zRSSwqyixLzIkvycxN1UlJLUgsKiktSoVwi0vywWrAdHFqYWlqXnIql5GFhYmBoYGOobGVoYGVATJDxxBZ1tQQKgthGOkYcQEAUEsBAhQDFAAAAAgALXh2Xf9JB/R0AAAAnwAAAA0AAAAAAAAAAAAAAIABAAAAAGZlZWRfaW5mby50eHRQSwECFAMUAAAACAAteHZd1gKO+R4AAAAgAAAACgAAAAAAAAAAAAAAgAGfAAAAcm91dGVzLnR4dFBLAQIUAxQAAAAIAC14dl3CXeNARgAAAFAAAAAJAAAAAAAAAAAAAACAAeUAAAB0cmlwcy50eHRQSwECFAMUAAAACAAteHZd4Ly99TMAAABDAAAACQAAAAAAAAAAAAAAgAFSAQAAc3RvcHMudHh0UEsBAhQDFAAAAAgALXh2XSQchn1QAAAAdgAAAA4AAAAAAAAAAAAAAIABrAEAAHN0b3BfdGltZXMudHh0UEsFBgAAAAAFAAUAHQEAACgCAAAAAA==', 'base64');

function harness(overrides = {}) {
  let time = new Date('2026-11-22T18:00:00Z');
  const oldRemote = { etag: 'old', lastModified: 'old date', contentLength: 100, sha256: 'old', downloadedAt: '2026-11-20T07:00:00Z' };
  const remote = { etag: 'new', lastModified: 'new date', contentLength: 200, sha256: 'new', downloadedAt: '2026-11-22T17:00:00Z' };
  let deployed = { feedVersion: 'S1000527', feedStartDate: '20261119', feedEndDate: '20261219', generatedAt: '2026-11-20T07:22:00Z', sourceRemote: oldRemote, tripCount: 79672 };
  const feed = { feedVersion: 'S1000528', feedStartDate: '20261120', feedEndDate: '20261220' };
  let control = { enabled: true, schedule: [{ id: 'daily', days: ['sunday'], time: '02:00' }], remote: oldRemote, index: deployed,
    lastScheduledCheckKey: '2026-11-20T02:20', lastRebuildRequestedAt: '', ...overrides.control };
  let runs = [];
  let conflict = null;
  let version = 1;
  const calls = [];
  let inspectCount = 0;
  let failHead = false;
  let failDispatch = false;
  let deploymentStatus = 'pending';
  const options = {
    readLocalControl: () => control, readDeployedIndex: () => deployed, token: 'fixture-only', repo: 'test/repo', branch: 'main', now: () => time,
    head: async () => { if (failHead) throw new Error('Official GTFS unavailable'); return { remote: { ...remote, sha256: undefined } }; },
    download: async () => { inspectCount += 1; return { remote, feed }; },
    github: async (method, resource, body) => {
      calls.push({ method, resource });
      if (method === 'GET' && resource.startsWith('contents/')) return { content: Buffer.from(JSON.stringify(control)).toString('base64'), sha: String(version) };
      if (method === 'PUT') {
        if (conflict) { const change = conflict; conflict = null; change(control); version += 1; const e = new Error('Conflict'); e.status = 409; throw e; }
        if (body.sha !== String(version)) { const e = new Error('Conflict'); e.status = 409; throw e; }
        control = JSON.parse(Buffer.from(body.content, 'base64')); version += 1;
        return {};
      }
      if (method === 'GET' && resource.startsWith('actions/')) return { workflow_runs: runs };
      if (method === 'POST') { if (failDispatch) throw new Error('GitHub dispatch unavailable'); return {}; }
      if (resource.startsWith('commits?')) return [{ sha: 'new-commit' }];
      if (resource.endsWith('/status')) return { state: deploymentStatus };
      throw new Error(`Unexpected fixture request: ${method} ${resource}`);
    },
    ...overrides.options,
  };
  return {
    service: createGtfsStaticAdmin(options), options, feed, remote, oldRemote, calls,
    get control() { return control; }, get deployed() { return deployed; },
    get inspectCount() { return inspectCount; },
    setDeployed: (value) => { deployed = value; }, setRuns: (value) => { runs = value; },
    setTime: (value) => { time = new Date(value); }, setConflict: (value) => { conflict = value; },
    failHead: () => { failHead = true; }, failDispatch: () => { failDispatch = true; },
    setDeploymentStatus: (value) => { deploymentStatus = value; },
  };
}

module.exports = { ZIP, harness };
