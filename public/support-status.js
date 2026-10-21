(function () {
  'use strict';
  if (window.OCSupport) return;
  let status = { enabled: false, kind: 'none', suppressPrompts: false };
  let token = null;
  let pending = null;
  let expiresTimer;
  let checkoutPoll;
  const CHECKOUT_KEY = 'oc-support-checkout-pending';
  const BANNER_DISMISSED_KEY = 'oc-supporter-banner-dismissed:v2';
  const BANNER_DISMISS_MS = 30 * 24 * 60 * 60 * 1000;
  let bannerDismissal = null;
  let bannerTimer;

  function getBannerDismissal() {
    try { return JSON.parse(localStorage.getItem(BANNER_DISMISSED_KEY) || 'null') || bannerDismissal; }
    catch (_) { return bannerDismissal; }
  }

  function isBannerDismissed() {
    const dismissal = getBannerDismissal();
    return dismissal?.kind === status.kind && Number.isFinite(dismissal.until) && dismissal.until > Date.now();
  }

  function setBannerDismissed(dismissed) {
    bannerDismissal = dismissed ? { kind: status.kind, until: Date.now() + BANNER_DISMISS_MS } : null;
    try {
      if (dismissed) localStorage.setItem(BANNER_DISMISSED_KEY, JSON.stringify(bannerDismissal));
      else localStorage.removeItem(BANNER_DISMISSED_KEY);
    } catch (_) {}
    render();
  }

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
    banner.hidden = !recognized || isBannerDismissed();
    clearTimeout(bannerTimer);
    if (recognized && isBannerDismissed()) {
      bannerTimer = setTimeout(() => refresh(), Math.min(2147483647, getBannerDismissal().until - Date.now() + 100));
    }
    if (recognized) {
      banner.dataset.kind = status.kind;
      const label = document.createElement('strong');
      label.textContent = status.kind === 'monthly' ? 'Monthly Supporter \u{1F49B}' : 'Thank you for your support \u{1F642}';
      if (status.kind === 'monthly' && Number.isFinite(Date.parse(status.supportingSince))) {
        const since = document.createElement('small');
        since.className = 'oc-supporter-since';
        since.textContent = `Supporting since ${new Intl.DateTimeFormat('en-CA', {
          timeZone: 'America/Toronto', month: 'long', year: 'numeric',
        }).format(new Date(status.supportingSince))}`;
        label.appendChild(since);
      }
      const message = document.createElement('span');
      message.textContent = status.kind === 'monthly'
        ? 'Your ongoing support keeps OC OPERATORS TOOLS running. With thanks, Omar.' : 'Thanks for helping keep these tools free.';
      const link = document.createElement('a');
      link.href = location.protocol === 'file:' ? 'support.html' : '/support';
      link.textContent = status.kind === 'monthly' ? 'Manage subscription' : 'Your support';
      const dismiss = document.createElement('button');
      dismiss.type = 'button';
      dismiss.className = 'oc-supporter-dismiss';
      dismiss.textContent = '\u00d7';
      dismiss.title = 'Hide thank-you banner';
      dismiss.setAttribute('aria-label', 'Hide thank-you banner');
      dismiss.addEventListener('click', () => setBannerDismissed(true));
      banner.replaceChildren(label, message, link, dismiss);
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

  window.OCSupport = { get status() { return status; }, get bannerDismissed() { return isBannerDismissed(); },
    showBanner: () => setBannerDismissed(false), refresh, ready: null };
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
  window.addEventListener('storage', (event) => {
    if (event.key === BANNER_DISMISSED_KEY || event.key === null) { bannerDismissal = null; render(); }
  });
  window.OCSupport.ready.then(() => checkAfterCheckout());
})();
