const crypto = require("node:crypto");

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
  const iso = new Date(exports.toEpochSeconds(timestamp) * 1000).toISOString();
  return iso.replace(/(-)|(T.+)/g, "");
};

/**
 * Get the YYYY-MM-DD date string for a timestamp
 */
exports.toISODateString = (timestamp) => {
  return exports.toISOExtendedZ(timestamp).split("T").shift();
};

/**
 * Get the ISO:Extended:Z date string for a timestamp
 */
exports.toISOExtendedZ = (timestamp) => {
  const iso = new Date(exports.toEpochSeconds(timestamp) * 1000).toISOString();
  return iso.replace(/\.000Z$/, "Z");
};

/**
 * Calculate a digest of any string and a UTC day
 */
exports.toDigest = (str, timestamp) => {
  const data = str + exports.toDateString(timestamp);
  return crypto.createHash("md5").update(data).digest("hex");
};
