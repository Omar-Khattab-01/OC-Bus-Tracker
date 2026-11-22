'use strict';

const {
  comparison, sameRemote, dueSchedule, sanitizeSchedule, officialRequest, downloadOfficial,
} = require('./gtfs_static_control');

function createGtfsStaticAdmin(options) {
  const { readLocalControl, readDeployedIndex, token, repo, branch } = options;
  const clock = options.now || (() => new Date());
  const github = options.github || (async (method, resource, body) => {
    const response = await fetch(`https://api.github.com/repos/${repo}/${resource}`, {
      method, signal: AbortSignal.timeout(15000),
      headers: { Accept: 'application/vnd.github+json', Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json', 'User-Agent': 'oc-bus-tracker-gtfs-admin', 'X-GitHub-Api-Version': '2022-11-28' },
      body: body ? JSON.stringify(body) : undefined,
    });
    const data = response.status === 204 ? {} : await response.json();
    if (!response.ok) {
      const error = new Error(`GitHub HTTP ${response.status}: ${String(data.message || 'Request failed').slice(0, 250)}`);
      error.status = response.status;
      throw error;
    }
    return data;
  });
  const controlPath = 'contents/data/gtfs_static_control.json';
  const ref = `ref=${encodeURIComponent(branch)}`;
  let inFlightRebuild = null;

  async function readStored() {
    if (!token) return { control: readLocalControl(), sha: null };
    const file = await github('GET', `${controlPath}?${ref}`);
    return { control: JSON.parse(Buffer.from(file.content, 'base64').toString('utf8')), sha: file.sha };
  }

  async function mutate(update, message) {
    if (!token) throw new Error('GitHub rebuild access is not configured.');
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const { control, sha } = await readStored();
      const next = update(control);
      if (!next) return control;
      try {
        await github('PUT', controlPath, { branch, sha, message,
          content: Buffer.from(`${JSON.stringify(next, null, 2)}\n`).toString('base64') });
        return next;
      } catch (error) {
        if (error.status !== 409 || attempt === 2) throw error;
      }
    }
  }

  async function workflowRuns() {
    const data = await github('GET', `actions/workflows/gtfs-static-index.yml/runs?branch=${encodeURIComponent(branch)}&per_page=5`);
    return data.workflow_runs || [];
  }

  async function pipeline(control, index) {
    if (!token || !control.lastRebuildRequestedAt || Date.parse(control.lastRebuildRequestedAt) <= Date.parse(index.generatedAt || 0)) return null;
    if (control.lastRebuildRequestStatus === 'dispatch-failed') {
      return { state: 'failed', label: 'Rebuild failed', message: control.lastRebuildError || 'The rebuild could not be started.' };
    }
    const runs = await workflowRuns();
    const run = runs.find((item) => Date.parse(item.created_at) >= Date.parse(control.lastRebuildRequestedAt) - 10000);
    if (run && run.status !== 'completed') {
      return { state: run.status === 'in_progress' ? 'rebuilding' : 'queued', label: run.status === 'in_progress' ? 'Rebuilding index' : 'Rebuild queued',
        message: 'The live website still uses the deployed index shown below.', url: run.html_url };
    }
    if (run && run.conclusion !== 'success') {
      return { state: 'failed', label: 'Rebuild failed', message: `GitHub workflow: ${run.conclusion}. The previous live index has not been replaced.`, url: run.html_url };
    }
    if (run?.conclusion === 'success') {
      const commits = await github('GET', `commits?path=data/gtfs_static_index.json.gz&sha=${encodeURIComponent(branch)}&per_page=1`);
      const latest = commits[0];
      const result = latest ? await github('GET', `commits/${latest.sha}/status`) : null;
      if (result?.state === 'failure' || result?.state === 'error') {
        return { state: 'failed', label: 'Deployment failed', message: 'The index built, but production deployment failed. The live website still uses the previous index.', url: run.html_url };
      }
      return { state: 'deployment_pending', label: 'Deployment pending', message: 'The index built successfully. Waiting for the live website to load that index.', url: run.html_url };
    }
    const timedOut = clock().valueOf() - Date.parse(control.lastRebuildRequestedAt) > 10 * 60000;
    return { state: timedOut ? 'failed' : 'queued', label: timedOut ? 'Rebuild not started' : 'Rebuild queued',
      message: timedOut ? 'No workflow appeared within 10 minutes. Retry the rebuild.' : 'Waiting for GitHub to start the rebuild.' };
  }

  async function response(control, extra = {}) {
    const index = readDeployedIndex();
    let progress;
    try { progress = await pipeline(control, index); }
    catch (error) { progress = { state: 'unverified', label: 'Unable to verify rebuild', message: error.message }; }
    const feedStatus = comparison(control, index, clock().valueOf());
    const realtime = control.realtime?.indexGeneratedAt === index.generatedAt ? control.realtime : null;
    let status = progress || feedStatus;
    if (!progress && feedStatus.state === 'up_to_date' && realtime?.withTrip >= 20 && realtime.matchRate < 0.8) {
      status = { state: 'live_mismatch', label: 'Live-feed mismatch', message: 'The live feed does not sufficiently match this index. The official download needs investigation; do not treat the index as verified.' };
    }
    return { ok: true, ...extra, control: { ...control, builtIndex: control.index, index, realtime, status, feedStatus, pipeline: progress,
      deploymentCommit: process.env.VERCEL_GIT_COMMIT_SHA || '' } };
  }

  async function status() { return response((await readStored()).control); }

  async function check({ persist = Boolean(token), scheduledKey = '', autoRebuild = false } = {}) {
    const stored = (await readStored()).control;
    const now = clock().toISOString();
    try {
      const { remote: head } = await (options.head || (() => officialRequest('HEAD')))();
      const stale = !stored.official?.verifiedAt || clock().valueOf() - Date.parse(stored.official.verifiedAt) >= 24 * 3600000;
      const realtime = options.verifyRealtime ? await options.verifyRealtime().catch((error) => ({ error: error.message, checkedAt: now })) : null;
      const liveMismatch = realtime?.withTrip >= 20 && realtime.matchRate < 0.8;
      const inspect = stale || liveMismatch || sameRemote(stored.official?.remote, head) !== true || !readDeployedIndex().sourceRemote;
      let official = stored.official;
      let remote = head;
      if (inspect) {
        const download = await (options.download || downloadOfficial)();
        if (sameRemote(head, download.remote) === false) {
          throw new Error('Official metadata and the downloaded ZIP disagree. The upstream download may be cached; the index has not been marked current. Retry the check.');
        }
        remote = download.remote;
        official = { ...download.feed, remote, verifiedAt: now };
      }
      const update = (current) => {
        if (Date.parse(current.lastCheckedAt) > Date.parse(now)) return current;
        const next = { ...current, remote, official, lastCheckedAt: now, lastAttemptAt: now, lastCheckError: '',
          ...(realtime ? { realtime: { ...realtime, indexGeneratedAt: readDeployedIndex().generatedAt } } : {}),
          ...(scheduledKey ? { lastScheduledCheckKey: scheduledKey } : {}) };
        if (comparison(next, readDeployedIndex(), clock().valueOf()).state === 'update_available') next.lastChangeDetectedAt = now;
        return next;
      };
      const control = persist ? await mutate(update, 'Record verified GTFS static feed check') : update(stored);
      if (autoRebuild && comparison(control, readDeployedIndex(), clock().valueOf()).state === 'update_available') {
        return await rebuild('scheduled-change-detected');
      }
      return response(control, { inspectedZip: inspect, changed: comparison(control, readDeployedIndex(), clock().valueOf()).state === 'update_available' });
    } catch (error) {
      if (persist) await mutate((current) => ({ ...current, lastAttemptAt: now, lastCheckError: error.message,
        ...(scheduledKey ? { lastScheduledCheckKey: stored.lastScheduledCheckKey || '' } : {}) }), 'Record failed GTFS static feed check');
      throw error;
    }
  }

  async function performRebuild(reason) {
    if (!token) throw new Error('GitHub rebuild access is not configured.');
    const active = (await workflowRuns()).find((run) => run.status !== 'completed');
    if (active) return response((await readStored()).control, { alreadyRunning: true, workflowUrl: active.html_url });
    const requestedAt = clock().toISOString();
    let claimed = false;
    const control = await mutate((current) => {
      claimed = false;
      const recent = clock().valueOf() - Date.parse(current.lastRebuildRequestedAt || 0) < 2 * 60000;
      if (recent && current.lastRebuildRequestStatus === 'workflow-dispatched') return null;
      claimed = true;
      return { ...current, lastRebuildRequestedAt: requestedAt, lastRebuildRequestStatus: 'workflow-dispatched', lastRebuildError: '' };
    }, 'Request GTFS static index rebuild');
    if (!claimed) return response(control, { alreadyRunning: true });
    try {
      await github('POST', 'actions/workflows/gtfs-static-index.yml/dispatches', { ref: branch, inputs: { reason } });
    } catch (error) {
      await mutate((current) => current.lastRebuildRequestedAt !== requestedAt ? null : { ...current,
        lastRebuildRequestStatus: 'dispatch-failed', lastRebuildError: error.message }, 'Record failed GTFS rebuild request');
      throw error;
    }
    return response(control, { dispatched: true });
  }

  async function rebuild(reason = 'manual-admin-request') {
    if (inFlightRebuild) return inFlightRebuild;
    inFlightRebuild = performRebuild(reason).finally(() => { inFlightRebuild = null; });
    return inFlightRebuild;
  }

  async function save(body) {
    const schedule = sanitizeSchedule(body.schedule);
    if (body.enabled && !schedule.length) throw new Error('Add a schedule before enabling automatic checks.');
    const control = await mutate((current) => ({ ...current, enabled: Boolean(body.enabled), timezone: 'America/Toronto', schedule }), 'Update GTFS static check schedule');
    return response(control);
  }

  async function cron() {
    if (!dueSchedule(readLocalControl(), clock())) return { ok: true, skipped: true };
    const control = (await readStored()).control;
    const due = dueSchedule(control, clock());
    if (!due) return { ok: true, skipped: true };
    return { ...await check({ persist: true, scheduledKey: due.current.key, autoRebuild: true }), skipped: false, due };
  }

  return { status, check, rebuild, save, cron };
}

module.exports = { createGtfsStaticAdmin };
