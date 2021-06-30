const log = require('lambda-log');

/**
 * Standard loggers (with prefixes so they're easy to search for)
 */
exports.log = function() { return log.info.apply(null, arguments); };
exports.debug = function() { return log.debug.apply(null, arguments); };
exports.info = function() { return log.info.apply(null, arguments); };
exports.warn = function() { return log.warn.apply(null, arguments); };
exports.error = function() { return log.error.apply(null, arguments); };

/**
 * Error decoding/logging (some bigquery errors are very nested)
 */
exports.errors = err => {
  if (err.response && err.response.insertErrors) {
    err.message = exports.combineErrors(err);
  } else if (err.reason && err.message) {
    err.message = `${err.reason} - ${err.message}`;
  }
  exports.error(err);
  return err;
}

/**
 * Combine insert errors into single string
 */
exports.combineErrors = original => {
  let msgs = [];
  ((original.response || {}).insertErrors || []).forEach(err => {
    if (err.errors && err.errors[0]) {
      if (err.errors[0].location && err.errors[0].message) {
        msgs.push(`${err.errors[0].location}: ${err.errors[0].message}`);
      } else if (err.errors[0].message) {
        msgs.push(err.errors[0].message);
      } else if (err.errors[0].reason) {
        msgs.push(err.errors[0].reason);
      }
    }
  });
  msgs = msgs.map(m => m.replace(/\.$/, ''));
  return msgs.filter((m, idx) => msgs.indexOf(m) === idx).join(', ');
}
