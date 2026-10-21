(function () {
  'use strict';
  if (window.OCSupport) return;
  let status = { enabled: false, kind: 'none', suppressPrompts: false };
  let token = null;
  let pending = null;
  let expiresTimer;
  let checkoutPoll;
  const CHECKOUT_KEY = 'oc-support-checkout-pending';

  function render() {
    const recognized = status.suppressPrompts && Date.parse(status.expiresAt) > Date.now();
    document.documentElement.classList.toggle('oc-is-supporter', Boolean(recognized));
    let banner = document.getElementById('ocSupporterBanner');
    if (!banner) {
      banner = document.createElement('div');
      banner.id = 'ocSupporterBanner';
      banner.className = 'oc-supporter-banner';
      banner.setAttribute('role', 'status');
      const target = document.querySelector('.oc-meta-bar');
      if (target) target.appendChild(banner);
      else document.body.prepend(banner);
    }
    banner.hidden = !recognized;
    if (recognized) {
      banner.dataset.kind = status.kind;
      const label = document.createElement('strong');
      label.textContent = status.kind === 'monthly' ? 'Monthly supporter' : 'Community supporter';
      const message = document.createElement('span');
      message.textContent = status.kind === 'monthly'
        ? 'Thank you for keeping these tools running.' : 'Thank you for supporting OC OPERATORS TOOLS.';
      const link = document.createElement('a');
      link.href = location.protocol === 'file:' ? 'support.html' : '/support';
      link.textContent = status.kind === 'monthly' ? 'Manage support' : 'Your support';
      banner.replaceChildren(label, message, link);
      const overlay = document.getElementById('ocSupportMessageOverlay');
      if (overlay) overlay.hidden = true;
    }
    clearTimeout(expiresTimer);
    if (recognized) expiresTimer = setTimeout(() => refresh(), Math.min(2147483647, Date.parse(status.expiresAt) - Date.now() + 100));
    window.dispatchEvent(new CustomEvent('oc:support-status', { detail: status }));
  }

  async function refresh(accessToken) {
    if (accessToken !== undefined) token = accessToken;
    if (pending) {
      await pending;
      if (accessToken === undefined) return status;
    }
    pending = (async () => {
      if (location.protocol === 'file:') return status;
      try {
        const response = await fetch('/api/support/status', {
          credentials: 'same-origin', cache: 'no-store', signal: AbortSignal.timeout(6000),
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        if (!response.ok) throw new Error('Status unavailable');
        status = await response.json();
        status.available = true;
      } catch (_) {
        status = { ...status, available: false };
      }
      render();
      return status;
    })();
    try { return await pending; } finally { pending = null; }
  }

  window.OCSupport = { get status() { return status; }, refresh, ready: null };
  window.OCSupport.ready = refresh();
  async function checkAfterCheckout(attempt = 0) {
    clearTimeout(checkoutPoll);
    let checkout;
    try { checkout = JSON.parse(localStorage.getItem(CHECKOUT_KEY) || 'null'); } catch (_) {}
    if (!checkout || Date.now() - checkout.at > 10 * 60000 || attempt >= 10) return;
    await refresh();
    if (status.suppressPrompts && (checkout.option !== 'monthly' || status.kind === 'monthly')) {
      try { localStorage.removeItem(CHECKOUT_KEY); } catch (_) {}
      return;
    }
    if (document.visibilityState === 'visible') checkoutPoll = setTimeout(() => checkAfterCheckout(attempt + 1), 3000);
  }
  document.addEventListener('click', (event) => {
    const link = event.target.closest('a[data-support-link]');
    if (!link) return;
    try { localStorage.setItem(CHECKOUT_KEY, JSON.stringify({ at: Date.now(), option: link.dataset.supportLink })); } catch (_) {}
  });
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') { refresh(); checkAfterCheckout(); }
    else clearTimeout(checkoutPoll);
  });
  window.OCSupport.ready.then(() => checkAfterCheckout());
})();
