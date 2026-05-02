(function () {
  const HOME_PATH = '/';
  const TIME_ZONE = 'America/Toronto';
  const MS_PER_DAY = 24 * 60 * 60 * 1000;
  const ANCHOR_SUNDAY_UTC = Date.UTC(2026, 3, 26);
  const ANCHOR_WEEK = 2;
  const PAY_WEEK = 1;
  const THURSDAY_INDEX = 4;
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

  ensureHeaderStyles();
  makeBrandClickable();
  injectMetaBar();

  window.__ocHeaderDebug = {
    getTorontoDateParts,
    getWeekNumber,
    addDaysParts,
    findNextPayDateParts
  };
})();
