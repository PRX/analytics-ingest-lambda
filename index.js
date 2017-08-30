'use strict';

const bigquery = require('./lib/bigquery');
const Inputs = require('./lib/inputs');

exports.handler = (event, context, callback) => {
  let errs = [];
  function concatErrors() {
    errs.forEach(e => exports.logError(`${e}`));
    if (errs.length === 1) {
      return errs[0];
    } else if (errs.length > 1) {
      let msgs = errs.map(e => `${e}`).join(', ');
      return new Error(`Multiple errors: ${msgs}`);
    } else {
      return null;
    }
  }

  // decode the base64 kinesis records
  if (!event || !event.Records) {
    let err = new Error(`Invalid event input: ${JSON.stringify(event)}`);
    exports.logError(`${err}`);
    return callback(err);
  }
  let records = event.Records.map(r => {
    try {
      return JSON.parse(new Buffer(r.kinesis.data, 'base64').toString('utf-8'));
    } catch (err) {
      errs.push(new Error(`Invalid record input: ${JSON.stringify(r)}`));
      return false;
    }
  }).filter(r => r);

  // input records and check for unrecognized
  let inputs = new Inputs(records);
  inputs.unrecognized.forEach(r => {
    errs.push(new Error(`Unrecognized input record: ${JSON.stringify(r)}`));
  });

  // run inserts in parallel
  inputs.insertAll().then(
    results => {
      let total = 0;
      results.forEach(result => {
        if (result.count > 0) {
          total += result.count;
          exports.logSuccess(`Inserted ${result.count} rows into ${result.dest}`);
        }
      });
      callback(concatErrors(), `Inserted ${total} rows`);
    },
    err => {
      errs = errs.concat(decodeErrors(err));
      callback(concatErrors());
    }
  );
};

// decode these crazy nested err.errors.errors
function decodeErrors(err) {
  if (err.errors) {
    return [].concat(err.errors.map(e => decodeErrors(e)));
  } else if (err.reason && err.message) {
    return new Error(`${err.reason} - ${err.message}`);
  } else {
    return err;
  }
}

// break out loggers so tests can silence them
exports.logSuccess = msg => console.log(msg);
exports.logError = msg => console.error(msg);
