(function () {
  'use strict';
  const notice = document.getElementById('supportManagementNotice');
  const manage = document.getElementById('manageSupportButton');
  const restore = document.getElementById('restoreSupportButton');
  const sync = document.getElementById('syncSupportButton');
  let client;
  let accessToken;

  function update(status) {
    const receipt = document.getElementById('supporterReceipt');
    if (status.portalUrl) manage.href = status.portalUrl;
    sync.hidden = !status.canSync;
    receipt.hidden = !status.suppressPrompts;
    if (status.suppressPrompts) {
      const monthly = status.kind === 'monthly';
      document.getElementById('supporterReceiptTitle').textContent = monthly ? 'Thank you for being a monthly supporter' : 'Thank you for your contribution';
      const date = new Date(status.expiresAt).toLocaleDateString('en-CA', { year: 'numeric', month: 'long', day: 'numeric' });
      document.getElementById('supporterReceiptMessage').textContent = monthly
        ? `Your support helps keep these tools running. ${status.cancelAtPeriodEnd ? `Your subscription will end on ${date}.` : 'Support reminders are hidden while your subscription is active.'}`
        : `Your support helps cover hosting, maintenance, and improvements. Your community supporter recognition and break from reminders last until ${date}.`;
    }
    document.querySelectorAll('[data-support-link]').forEach((link) => {
      if (link.hostname !== 'buy.stripe.com') return;
      const url = new URL(link.href);
      if (status.checkoutReference) url.searchParams.set('client_reference_id', status.checkoutReference);
      else url.searchParams.delete('client_reference_id');
      link.href = url.href;
    });
  }

  window.addEventListener('oc:support-status', (event) => update(event.detail));

  document.getElementById('restoreSupportForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    if (!client || !window.OCSupport?.status.enabled) {
      notice.textContent = 'Email verification is not available yet. Please try again later.';
      return;
    }
    restore.disabled = true;
    try {
      const { error } = await client.auth.signInWithOtp({ email: document.getElementById('supporterEmail').value.trim(),
        options: { emailRedirectTo: `${location.origin}/support?restore=1` } });
      if (error) throw error;
      notice.textContent = 'Check your email for a verification link. Open it on this device to restore your supporter recognition.';
    } catch (error) { notice.textContent = error.message || 'Unable to send verification email. Please try again.'; }
    finally { restore.disabled = false; }
  });

  sync.addEventListener('click', async () => {
    if (!accessToken) return;
    sync.disabled = true;
    let cursor;
    let checked = 0;
    try {
      do {
        const response = await fetch('/api/support/sync', { method: 'POST', signal: AbortSignal.timeout(55000),
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
          body: JSON.stringify({ cursor }) });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error);
        checked += data.checked;
        cursor = data.nextCursor;
        notice.textContent = `Checked ${checked} completed checkouts${cursor ? '...' : '. Existing contributions are now synced.'}`;
      } while (cursor);
      await window.OCSupport.refresh(accessToken);
    } catch (error) { notice.textContent = error.message || 'Import paused. You can safely try again.'; }
    finally { sync.disabled = false; }
  });

  async function init() {
    // The shared header loads supporter status asynchronously.
    if (!window.OCSupport) {
      await new Promise((resolve) => {
        const onReady = () => { window.removeEventListener('oc:support-status', onReady); resolve(); };
        window.addEventListener('oc:support-status', onReady);
        setTimeout(() => { window.removeEventListener('oc:support-status', onReady); resolve(); }, 6500);
      });
    }
    if (window.OCSupport) { await window.OCSupport.ready; update(window.OCSupport.status); }
    try {
      const response = await fetch('/api/supabase-config', { signal: AbortSignal.timeout(6000) });
      const config = await response.json();
      if (!config.enabled || !window.supabase) return;
      client = window.supabase.createClient(config.url, config.anonKey);
      async function restoreSession(session) {
        accessToken = session?.access_token || null;
        if (!accessToken || !window.OCSupport) return;
        const status = await window.OCSupport.refresh(accessToken);
        if (new URLSearchParams(location.search).has('restore')) {
          notice.textContent = status.suppressPrompts ? 'Your supporter recognition has been restored on this device. Thank you!'
            : 'Email verified. No current supporter recognition was found. Please use the same email you used at checkout.';
        }
      }
      const { data } = await client.auth.getSession();
      await restoreSession(data.session);
      client.auth.onAuthStateChange((_event, session) => { setTimeout(() => restoreSession(session), 0); });
    } catch (_) { /* Payment links remain available when email verification is unavailable. */ }
  }
  init();
})();
