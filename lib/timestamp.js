import crypto from "node:crypto";

// in the year 2000. in the year 2000.
const THRESHOLD = 946684800000;

/**
 * Guess if a timestamp is milliseconds or seconds
 */
export const toEpochSeconds = (timestamp) => {
  if (timestamp > THRESHOLD) {
    return Math.floor(timestamp / 1000);
  } else {
    return timestamp;
  }
};

/**
 * Convert to milliseconds
 */
export const toEpochMilliseconds = (timestamp) => {
  if (timestamp > THRESHOLD) {
    return timestamp;
  } else {
    return timestamp * 1000;
  }
};

/**
 * Get the YYYYMMDD date string for a timestamp
 */
export const toDateString = (timestamp) => {
  const iso = new Date(toEpochSeconds(timestamp) * 1000).toISOString();
  return iso.replace(/(-)|(T.+)/g, "");
};

/**
 * Get the YYYY-MM-DD date string for a timestamp
 */
export const toISODateString = (timestamp) => {
  return toISOExtendedZ(timestamp).split("T").shift();
};

/**
 * Get the ISO:Extended:Z date string for a timestamp
 */
export const toISOExtendedZ = (timestamp) => {
  const iso = new Date(toEpochMilliseconds(timestamp)).toISOString();
  return iso.replace(/\.000Z$/, "Z");
};

/**
 * Calculate a digest of any string and a UTC day
 */
export const toDigest = (str, timestamp) => {
  const data = str + toDateString(timestamp);
  return crypto.createHash("md5").update(data).digest("hex");
};
