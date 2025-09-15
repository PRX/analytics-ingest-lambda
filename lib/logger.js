/**
 * Standard loggers (with prefixes so they're easy to search for)
 */
export const log = (...args) => log.info.apply(null, args);

export const debug = (...args) => log.debug.apply(null, args);
export const info = (...args) => log.info.apply(null, args);
export const warn = (...args) => log.warn.apply(null, args);
export const error = (...args) => log.error.apply(null, args);

/**
 * Error decoding/logging (some bigquery errors are very nested)
 */
export const errors = (err) => {
  if (err.response?.insertErrors) {
    err.message = combineErrors(err);
  } else if (err.reason && err.message) {
    err.message = `${err.reason} - ${err.message}`;
  }
  if (!err.skipLogging) {
    error(err);
  }
  return err;
};

/**
 * Combine insert errors into single string
 */
export const combineErrors = (original) => {
  let msgs = [];
  (original.response?.insertErrors || []).forEach((err) => {
    if (err.errors?.[0]) {
      if (err.errors[0].location && err.errors[0].message) {
        msgs.push(`${err.errors[0].location}: ${err.errors[0].message}`);
      } else if (err.errors[0].message) {
        msgs.push(err.errors[0].message);
      } else if (err.errors[0].reason) {
        msgs.push(err.errors[0].reason);
      }
    }
  });
  msgs = msgs.map((m) => m.replace(/\.$/, ""));
  return msgs.filter((m, idx) => msgs.indexOf(m) === idx).join(", ");
};
