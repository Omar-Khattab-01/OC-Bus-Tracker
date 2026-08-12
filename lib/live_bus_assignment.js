'use strict';

const DEFAULT_MAX_ASSIGNMENT_AGE_MS = 30 * 60 * 60 * 1000;

function getOttawaDateString(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function isRetainableAssignment(mapping, context = {}) {
  if (!mapping?.block || !mapping?.verifiedAt) return false;
  const now = context.now instanceof Date ? context.now : new Date(context.now || Date.now());
  const verifiedAt = new Date(mapping.verifiedAt);
  if (Number.isNaN(now.getTime()) || Number.isNaN(verifiedAt.getTime())) return false;

  const ageMs = now.getTime() - verifiedAt.getTime();
  const maxAgeMs = Number(context.maxAgeMs || DEFAULT_MAX_ASSIGNMENT_AGE_MS);
  if (ageMs < -5 * 60 * 1000 || ageMs > maxAgeMs) return false;
  if (context.afterFinalTrip) return false;

  const verifiedServiceDate = getOttawaDateString(verifiedAt);
  const currentServiceDate = getOttawaDateString(now);
  if (verifiedServiceDate === currentServiceDate) return true;

  const previousServiceDate = getOttawaDateString(new Date(now.getTime() - 24 * 60 * 60 * 1000));
  return Boolean(context.paddleCarryover && verifiedServiceDate === previousServiceDate);
}

function selectNewestAssignments(mappings, context = {}) {
  const eligible = (Array.isArray(mappings) ? mappings : [])
    .filter((mapping) => isRetainableAssignment(mapping, context))
    .sort((a, b) => Date.parse(b.verifiedAt) - Date.parse(a.verifiedAt));
  if (!eligible.length) return [];

  const newestTimestamp = Date.parse(eligible[0].verifiedAt);
  return eligible.filter((mapping) => Date.parse(mapping.verifiedAt) === newestTimestamp);
}

function canUseRetainedAssignmentForPosition(mapping, position) {
  const tripId = String(position?.tripId || position?.trip_id || '').trim();
  if (!tripId) return true;
  const mappedBlock = String(mapping?.block || '').trim().toUpperCase();
  const positionBlock = String(position?.blockId || position?.block_id || '').trim().toUpperCase();
  return Boolean(mappedBlock && positionBlock && mappedBlock === positionBlock);
}

module.exports = {
  DEFAULT_MAX_ASSIGNMENT_AGE_MS,
  canUseRetainedAssignmentForPosition,
  getOttawaDateString,
  isRetainableAssignment,
  selectNewestAssignments,
};
