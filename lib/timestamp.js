'use strict';

// in the year 2000. in the year 2000.
const THRESHOLD = 946684800000;

/**
 * Guess if a timestamp is milliseconds or seconds
 */
exports.toEpochSeconds = (timestamp) => {
  if (timestamp > THRESHOLD) {
    return Math.floor(timestamp / 1000);
  } else {
    return timestamp;
  }
};

/**
 * Convert to milliseconds
 */
exports.toEpochMilliseconds = (timestamp) => {
  if (timestamp > THRESHOLD) {
    return timestamp;
  } else {
    return timestamp * 1000;
  }
};

/**
 * Get the YYYYMMDD date string for a timestamp
 */
exports.toDateString = (timestamp) => {
  let iso = new Date(exports.toEpochSeconds(timestamp) * 1000).toISOString();
  return iso.replace(/(-)|(T.+)/g, '');
};
