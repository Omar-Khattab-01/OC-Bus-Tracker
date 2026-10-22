'use strict';

const activeTrip = { tripNumber: 6, route: '17', headsign: 'Wateridge', startTime: '17:28', endTime: '17:59' };
const paddle = {
  ok: true, block: '44-7', serviceDay: 'weekday', variantId: 'fall', sourceLabel: 'Weekdays',
  effective: 'August 31, 2026', paddleId: '044007', routes: ['17', '38', '44'],
  garage: 'St L South Garage', signOn: '12:29', busType: 'ELNT', isLiveDay: true, activeTrip,
  trips: [
    { trip_number: 6, route: '17', headsign: 'Wateridge', start_time: '17:28', end_time: '17:59',
      start_stop: 'QUEEN / KENT', end_stop: 'Wateridge', breakAfterMinutes: 8 },
    { trip_number: 9, route: '38', headsign: 'Blair Station', start_time: '19:45', end_time: '20:18',
      start_stop: 'Place d\u0027Orleans', end_stop: 'Blair Station' },
  ],
};
const live = {
  ok: true, mode: 'block', block: '44-7', currentTrip: { ...activeTrip, label: '17 Wateridge' },
  buses: [{ busNumber: '2102', locationText: 'at QUEEN / KENT', latitude: null, longitude: null }],
  paddleAvailable: true,
  paddleOptions: [{ serviceDay: 'weekday', variantId: 'fall', buttonLabel: 'Weekday Paddle' }],
};
module.exports = { paddle, live };
