(function () {
  const HOME_PATH = '/';
  const TIME_ZONE = 'America/Toronto';
  const MS_PER_DAY = 24 * 60 * 60 * 1000;
  const ANCHOR_SUNDAY_UTC = Date.UTC(2026, 3, 26);
  const ANCHOR_WEEK = 2;
  const PAY_WEEK = 1;
  const THURSDAY_INDEX = 4;
  const SUPPORT_MESSAGE_DISMISS_UNTIL_KEY = 'oc-operators-tools:support-message-dismissed-until:v1';
  const SUPPORT_MESSAGE_DISMISS_DAYS = 14;
  const SUPPORT_MESSAGE_CLICK_DAYS = 30;
  const DAY_NAMES = {
    Sun: 'Sunday',
    Mon: 'Monday',
    Tue: 'Tuesday',
    Wed: 'Wednesday',
    Thu: 'Thursday',
    Fri: 'Friday',
    Sat: 'Saturday'
  };
  const WEEKDAY_TO_INDEX = {
    Sun: 0,
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6
  };

  function ensureHeaderStyles() {
    if (document.getElementById('ocSharedHeaderStyles')) return;

    const style = document.createElement('style');
    style.id = 'ocSharedHeaderStyles';
    style.textContent = `
      .brand-link {
        display: inline-flex;
        align-items: center;
        gap: inherit;
        color: inherit;
        text-decoration: none;
        min-width: 0;
      }

      .brand-link:hover .badge,
      .brand-link:focus-visible .badge {
        transform: translateY(-1px);
        box-shadow: 0 8px 18px rgba(218, 41, 28, 0.28);
      }

      .brand-link:focus-visible {
        outline: 2px solid rgba(255, 255, 255, 0.7);
        outline-offset: 6px;
        border-radius: 14px;
      }

      .badge {
        transition: transform 120ms ease, box-shadow 120ms ease;
      }

      .app.oc-has-meta-bar {
        grid-template-rows: auto auto minmax(0, 1fr) !important;
      }

      .oc-meta-bar {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 8px 12px;
        padding: 8px 18px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.12);
        background: linear-gradient(90deg, rgba(255, 255, 255, 0.06), rgba(255, 255, 255, 0.02));
        color: inherit;
      }

      .oc-meta-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        min-height: 28px;
        padding: 5px 10px;
        border: 1px solid rgba(255, 255, 255, 0.14);
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.04);
        font-size: 12px;
        line-height: 1.2;
        white-space: nowrap;
      }

      .oc-meta-label {
        color: rgba(215, 221, 230, 0.82);
      }

      .oc-meta-value {
        font-weight: 600;
      }

      .oc-support-footer {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        padding: 14px 18px 18px;
        border-top: 1px solid rgba(255, 255, 255, 0.08);
        background: rgba(12, 16, 21, 0.38);
        color: rgba(215, 221, 230, 0.82);
        font-size: 12px;
        line-height: 1.4;
        flex-wrap: wrap;
      }

      .oc-support-footer a {
        color: #ffcf7a;
        font-weight: 600;
        text-decoration: none;
      }

      .oc-support-footer a:hover {
        text-decoration: underline;
      }

      .oc-support-message-overlay[hidden] {
        display: none;
      }

      .oc-support-message-overlay {
        position: fixed;
        inset: 0;
        z-index: 1000;
        display: grid;
        place-items: center;
        padding: 18px;
        background: rgba(5, 8, 12, 0.72);
      }

      .oc-support-message-modal {
        width: min(620px, 100%);
        max-height: min(86vh, 900px);
        overflow: hidden;
        border: 1px solid rgba(255, 255, 255, 0.14);
        border-radius: 22px;
        background: linear-gradient(180deg, rgba(24, 31, 39, 0.98), rgba(15, 19, 24, 0.98));
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.35);
        color: #f8fafc;
        display: grid;
        grid-template-rows: auto 1fr;
      }

      .oc-support-message-head {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 16px;
        padding: 16px 18px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.14);
        background: linear-gradient(110deg, rgba(218, 41, 28, 0.14), rgba(15, 17, 21, 0.2));
      }

      .oc-support-message-head h2 {
        margin: 0 0 4px;
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 30px;
        line-height: 1;
        letter-spacing: 0.4px;
        text-transform: uppercase;
      }

      .oc-support-message-head p {
        margin: 0;
        color: #d7dde6;
        font-size: 13px;
      }

      .oc-support-message-close {
        appearance: none;
        border: 1px solid rgba(255, 255, 255, 0.14);
        background: rgba(10, 14, 18, 0.82);
        color: #f8fafc;
        border-radius: 999px;
        min-width: 38px;
        min-height: 38px;
        font: inherit;
        font-size: 20px;
        cursor: pointer;
      }

      .oc-support-message-body {
        overflow: auto;
        padding: 22px;
        display: grid;
        gap: 18px;
      }

      .oc-support-message-copy {
        display: grid;
        gap: 12px;
        color: #d7dde6;
        font-size: 15px;
        line-height: 1.55;
      }

      .oc-support-message-copy p {
        margin: 0;
      }

      .oc-support-message-copy strong {
        color: #f8fafc;
      }

      .oc-support-message-actions {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
      }

      .oc-support-message-primary,
      .oc-support-message-secondary {
        min-height: 42px;
        border-radius: 999px;
        padding: 8px 14px;
        font: inherit;
        font-size: 13px;
        font-weight: 700;
        cursor: pointer;
        text-decoration: none;
        display: inline-flex;
        align-items: center;
        justify-content: center;
      }

      .oc-support-message-primary {
        border: 0;
        color: #fff;
        background: linear-gradient(135deg, #e53224 0%, #be2318 100%);
      }

      .oc-support-message-secondary {
        border: 1px solid rgba(255, 255, 255, 0.14);
        color: #9ec2ff;
        background: rgba(15, 18, 24, 0.72);
      }

      .oc-support-message-note {
        color: rgba(220, 228, 238, 0.72);
        font-size: 12px;
      }

      @media (max-width: 640px) {
        .oc-meta-bar {
          flex-wrap: nowrap;
          gap: 6px;
          padding: 7px 10px;
          overflow-x: auto;
          scrollbar-width: none;
        }

        .oc-meta-bar::-webkit-scrollbar {
          display: none;
        }

        .oc-meta-pill {
          flex: 0 0 auto;
          min-width: max-content;
          min-height: 24px;
          padding: 4px 8px;
          font-size: 10px;
        }

        .brand-link:focus-visible {
          outline-offset: 4px;
        }

        .oc-support-footer {
          padding: 12px;
          font-size: 11px;
        }

        .oc-support-message-body {
          padding: 16px;
        }
      }
    `;

    document.head.appendChild(style);
  }

  function getTorontoDateParts(date) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
      timeZone: TIME_ZONE,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      weekday: 'short'
    });
    const values = {};
    for (const part of formatter.formatToParts(date)) {
      if (part.type !== 'literal') values[part.type] = part.value;
    }
    return {
      year: Number(values.year),
      month: Number(values.month),
      day: Number(values.day),
      weekday: values.weekday
    };
  }

  function torontoPartsToUtcMs(parts) {
    return Date.UTC(parts.year, parts.month - 1, parts.day);
  }

  function getWeekNumber(parts) {
    const weekdayIndex = WEEKDAY_TO_INDEX[parts.weekday];
    const currentDayUtc = torontoPartsToUtcMs(parts);
    const sundayUtc = currentDayUtc - (weekdayIndex * MS_PER_DAY);
    const weekOffset = Math.round((sundayUtc - ANCHOR_SUNDAY_UTC) / (7 * MS_PER_DAY));
    const normalized = (ANCHOR_WEEK - 1 + weekOffset) % 2;
    return normalized < 0 ? normalized + 3 : normalized + 1;
  }

  function addDaysParts(parts, daysToAdd) {
    const nextUtc = torontoPartsToUtcMs(parts) + (daysToAdd * MS_PER_DAY);
    return getTorontoDateParts(new Date(nextUtc + 12 * 60 * 60 * 1000));
  }

  function formatPayDate(parts) {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: TIME_ZONE,
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    }).format(new Date(Date.UTC(parts.year, parts.month - 1, parts.day, 12)));
  }

  function findNextPayDateParts(todayParts) {
    for (let offset = 0; offset < 28; offset += 1) {
      const candidate = addDaysParts(todayParts, offset);
      if (WEEKDAY_TO_INDEX[candidate.weekday] !== THURSDAY_INDEX) continue;
      if (getWeekNumber(candidate) !== PAY_WEEK) continue;
      return candidate;
    }
    return null;
  }

  function buildMetaBar() {
    const todayParts = getTorontoDateParts(new Date());
    const tomorrowParts = addDaysParts(todayParts, 1);
    const payDateParts = findNextPayDateParts(todayParts);
    const todayWeek = `${DAY_NAMES[todayParts.weekday] || todayParts.weekday} ${getWeekNumber(todayParts)}`;
    const tomorrowWeek = `${DAY_NAMES[tomorrowParts.weekday] || tomorrowParts.weekday} ${getWeekNumber(tomorrowParts)}`;

    const bar = document.createElement('div');
    bar.className = 'oc-meta-bar';
    bar.setAttribute('role', 'note');
    bar.setAttribute('aria-label', 'OC week and pay information');
    bar.innerHTML = `
      <div class="oc-meta-pill"><span class="oc-meta-value">Today: ${todayWeek}</span></div>
      <div class="oc-meta-pill"><span class="oc-meta-value">Tomorrow: ${tomorrowWeek}</span></div>
      <div class="oc-meta-pill"><span class="oc-meta-value">Next Pay: ${payDateParts ? formatPayDate(payDateParts) : 'Unavailable'}</span></div>
    `;
    return bar;
  }

  function makeBrandClickable() {
    const brand = document.querySelector('.header .brand');
    if (!brand || brand.closest('a.brand-link')) return;

    const link = document.createElement('a');
    link.href = HOME_PATH;
    link.className = `${brand.className} brand-link`;
    link.setAttribute('aria-label', 'Go to main bus lookup');
    link.innerHTML = brand.innerHTML;
    brand.replaceWith(link);
  }

  function injectMetaBar() {
    if (document.querySelector('.oc-meta-bar')) return;

    const app = document.querySelector('.app');
    const header = document.querySelector('.app > .header');
    if (!app || !header) return;

    app.classList.add('oc-has-meta-bar');
    app.insertBefore(buildMetaBar(), header);
  }

  function getSupportPath() {
    return window.location.protocol === 'file:' ? 'support.html' : '/support';
  }

  function isSupportPage() {
    const pathname = String(window.location.pathname || '').toLowerCase();
    return pathname === '/support' || pathname.endsWith('/support') || pathname.endsWith('/support.html');
  }

  function readLocalStorageValue(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (_) {
      return null;
    }
  }

  function writeLocalStorageValue(key, value) {
    try {
      window.localStorage.setItem(key, value);
    } catch (_) {}
  }

  function addDaysFromNow(days) {
    return Date.now() + (Number(days) || 0) * MS_PER_DAY;
  }

  function isSupportMessageDismissed() {
    const value = Number(readLocalStorageValue(SUPPORT_MESSAGE_DISMISS_UNTIL_KEY) || 0);
    return Number.isFinite(value) && value > Date.now();
  }

  function dismissSupportMessage(days) {
    const overlay = document.getElementById('ocSupportMessageOverlay');
    if (overlay) overlay.hidden = true;
    writeLocalStorageValue(SUPPORT_MESSAGE_DISMISS_UNTIL_KEY, String(addDaysFromNow(days)));
  }

  function updateSupportLinks() {
    const supportPath = getSupportPath();
    document.querySelectorAll('a[href="/support"]').forEach((link) => {
      link.href = supportPath;
    });

    const supportButtons = [
      document.getElementById('supportBtn'),
      document.getElementById('mobileSupportBtn')
    ].filter(Boolean);

    supportButtons.forEach((button) => {
      if (button.dataset.ocSupportBound === '1') return;
      button.dataset.ocSupportBound = '1';
      button.addEventListener('click', () => {
        window.location.href = supportPath;
      });
    });
  }

  function buildSupportMessage() {
    const overlay = document.createElement('div');
    overlay.id = 'ocSupportMessageOverlay';
    overlay.className = 'oc-support-message-overlay';
    overlay.hidden = true;
    overlay.innerHTML = `
      <section class="oc-support-message-modal" role="dialog" aria-modal="true" aria-labelledby="ocSupportMessageTitle">
        <header class="oc-support-message-head">
          <div>
            <h2 id="ocSupportMessageTitle">A Message From Omar</h2>
            <p>OC OPERATORS TOOLS remains free to use.</p>
          </div>
          <button class="oc-support-message-close" type="button" aria-label="Close support message">x</button>
        </header>
        <div class="oc-support-message-body">
          <div class="oc-support-message-copy">
            <p><strong>If you use this site and benefit from it, please consider supporting the work behind it.</strong></p>
            <p>OC OPERATORS TOOLS is maintained only by me. I’ve put many hours into building it, improving it, and keeping it running.</p>
            <p>Support helps cover hosting, database costs, maintenance, and future improvements. Contributions are completely optional, but they help keep the site available and improving.</p>
          </div>
          <div class="oc-support-message-actions">
            <a class="oc-support-message-primary" href="${getSupportPath()}">Support the site</a>
            <button class="oc-support-message-secondary" type="button">Maybe later</button>
          </div>
          <div class="oc-support-message-note">You can close this and continue using the site.</div>
        </div>
      </section>
    `;
    return overlay;
  }

  function initSupportMessage() {
    updateSupportLinks();
    if (isSupportPage() || document.getElementById('ocSupportMessageOverlay')) return;

    if (new URLSearchParams(window.location.search || '').get('showSupportMessage') === '1') {
      writeLocalStorageValue(SUPPORT_MESSAGE_DISMISS_UNTIL_KEY, '0');
    }

    const overlay = buildSupportMessage();
    const closeButton = overlay.querySelector('.oc-support-message-close');
    const laterButton = overlay.querySelector('.oc-support-message-secondary');
    const supportLink = overlay.querySelector('.oc-support-message-primary');

    closeButton.addEventListener('click', () => dismissSupportMessage(SUPPORT_MESSAGE_DISMISS_DAYS));
    laterButton.addEventListener('click', () => dismissSupportMessage(SUPPORT_MESSAGE_DISMISS_DAYS));
    supportLink.addEventListener('click', () => {
      writeLocalStorageValue(SUPPORT_MESSAGE_DISMISS_UNTIL_KEY, String(addDaysFromNow(SUPPORT_MESSAGE_CLICK_DAYS)));
    });
    overlay.addEventListener('click', (event) => {
      if (event.target === overlay) dismissSupportMessage(SUPPORT_MESSAGE_DISMISS_DAYS);
    });
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && !overlay.hidden) {
        dismissSupportMessage(SUPPORT_MESSAGE_DISMISS_DAYS);
      }
    });

    document.body.appendChild(overlay);
    if (!isSupportMessageDismissed()) {
      window.setTimeout(() => {
        overlay.hidden = false;
      }, 550);
    }
  }

  ensureHeaderStyles();
  makeBrandClickable();
  injectMetaBar();
  initSupportMessage();

  window.__ocHeaderDebug = {
    getTorontoDateParts,
    getWeekNumber,
    addDaysParts,
    findNextPayDateParts,
    dismissSupportMessage,
    isSupportMessageDismissed
  };
})();
