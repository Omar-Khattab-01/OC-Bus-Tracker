'use strict';

const GARAGE_NAMES = ['Industrial Garage', 'Merivale Garage', 'Pinecrest Garage', 'St L South Garage', 'St L North Garage'];
const POST_TRIP_CHECK = /\s*(Conduct Post Trip Checks at the End of Each Trip and at the End of the Line)\.?\s*$/i;

function clockMinutes(value) {
  const match = String(value || '').trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!match || Number(match[1]) > 47 || Number(match[2]) > 59) return null;
  return Number(match[1]) * 60 + Number(match[2]);
}

function formatClock(value) {
  return `${String(Math.floor(value / 60) % 24).padStart(2, '0')}:${String(value % 60).padStart(2, '0')}`;
}

function getGarageReturn(trip, garage) {
  if (!trip) return null;
  const text = String(trip.next_directions || '').trim();
  const names = [...new Set([...GARAGE_NAMES, /\bgarage$/i.test(String(garage || '').trim()) ? String(garage).trim() : ''])]
    .filter(Boolean).sort((a, b) => b.length - a.length);
  for (const name of names) {
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const match = text.match(new RegExp(`\\b(${escaped})\\s*(\\d{1,2}:\\d{2})\\s*$`, 'i'));
    if (!match) continue;
    const arrival = clockMinutes(match[2]);
    if (arrival === null) return null;
    const departure = clockMinutes(trip.end_time);
    // Clock times wrap at midnight; this is the scheduled end-to-arrival interval.
    const duration = departure === null ? null : ((arrival - departure) % 1440 + 1440) % 1440;
    const precedingText = text.slice(0, match.index).trim();
    const check = precedingText.match(POST_TRIP_CHECK);
    return {
      garage: match[1], arrivalTime: formatClock(arrival), durationMinutes: duration,
      directions: precedingText.replace(POST_TRIP_CHECK, '').trim(),
      postTripChecks: check ? check[1] : null, isDeadhead: true,
    };
  }
  const endStop = String(trip.end_stop || '').trim();
  const arrival = clockMinutes(trip.end_time);
  if (!text && names.some((name) => name.toLowerCase() === endStop.toLowerCase()) && arrival !== null) {
    return { garage: endStop, arrivalTime: formatClock(arrival), durationMinutes: null,
      directions: '', postTripChecks: null, isDeadhead: false };
  }
  return null;
}

module.exports = { getGarageReturn };
