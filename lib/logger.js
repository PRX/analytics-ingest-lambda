/**
 * Standard loggers (with prefixes so they're easy to search for)
 */
exports.log = msg => console.log('[LOG]', msg);
exports.info = msg => console.info('[INFO]', msg);
exports.warn = msg => console.warn('[WARN]', msg);
exports.error = msg => console.error('[ERROR]', msg);

/**
 * Error decoding/logging (some bigquery errors are very nested)
 */
exports.errors = err => {
  let errs = [];
  if (err.errors) {
    errs = errs.concat(err.errors.map(e => decodeErrors(e)));
  } else if (err.reason && err.message) {
    errs.push(new Error(`${err.reason} - ${err.message}`));
  } else {
    errs.push(err);
  }
  errs.forEach(e => exports.error(`${e}`));

  // return a single error
  if (errs.length === 1) {
    return errs[0];
  } else if (errs.length > 1) {
    let msgs = errs.map(e => `${e}`).join(', ');
    return new Error(`Multiple errors: ${msgs}`);
  } else {
    return null;
  }
}
