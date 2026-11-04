(function (root) {
  'use strict';
  const WINTER_START = '2026-12-20';
  const REGULAR_WEEKDAY_START = '2027-01-04';
  const dateFormatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto', year: 'numeric', month: '2-digit', day: '2-digit',
  });

  function isWinterActive(date = new Date()) {
    return dateFormatter.format(date) >= WINTER_START;
  }

  function winterServiceDay(serviceDay, date = new Date()) {
    const day = dateFormatter.format(date);
    if (serviceDay !== 'weekday' || day < WINTER_START) return serviceDay;
    if (['2026-12-25', '2027-01-01', '2027-03-26'].includes(day)) return 'sunday';
    if (day === '2026-12-28') return 'special_service';
    if ((day >= '2026-12-20' && day <= '2027-01-02')
      || (day >= '2027-03-14' && day <= '2027-03-20')
      || day === '2027-03-29') return 'reduced_week';
    return serviceDay;
  }

  const api = { WINTER_START, REGULAR_WEEKDAY_START, isWinterActive, winterServiceDay };
  if (typeof module === 'object' && module.exports) module.exports = api;
  else {
    root.OCPaddleSeason = api;
    const style = document.createElement('style');
    style.textContent = '[data-winter-preview][hidden] { display: none !important; }';
    document.head.appendChild(style);
    const updateNavigation = () => {
      document.querySelectorAll('[data-winter-preview]').forEach((element) => {
        element.hidden = isWinterActive();
      });
    };
    updateNavigation();
    setInterval(updateNavigation, 60000);
    document.addEventListener('visibilitychange', updateNavigation);
  }
})(typeof window === 'object' ? window : globalThis);
