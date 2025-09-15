const log = require("lambda-log");

/**
 * Standard loggers (with prefixes so they're easy to search for)
 */
exports.log = (...args) => log.info.apply(null, args);
exports.debug = (...args) => log.debug.apply(null, args);
exports.info = (...args) => log.info.apply(null, args);
exports.warn = (...args) => log.warn.apply(null, args);
exports.error = (...args) => log.error.apply(null, args);

/**
 * Error decoding/logging (some bigquery errors are very nested)
 */
exports.errors = (err) => {
  if (err.response?.insertErrors) {
    err.message = exports.combineErrors(err);
  } else if (err.reason && err.message) {
    err.message = `${err.reason} - ${err.message}`;
  }
  if (!err.skipLogging) {
    exports.error(err);
  }
  return err;
};

/**
 * Combine insert errors into single string
 */
exports.combineErrors = (original) => {
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
