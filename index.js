'use strict';

const logger = require('./lib/logger');
const Inputs = require('./lib/inputs');

exports.handler = (event, context, callback) => {
  let fatalErr, records;

  // decode the base64 kinesis records
  if (!event || !event.Records) {
    fatalErr = `Invalid event input: ${JSON.stringify(event)}`;
  } else {
    records = event.Records.map(r => {
      try {
        return JSON.parse(new Buffer(r.kinesis.data, 'base64').toString('utf-8'));
      } catch (decodeErr) {
        fatalErr = `Invalid record input: ${JSON.stringify(r)}`;
      }
    });
  }

  // NOTE: callback with errors here will cause the lambda to run indefinitely
  // on the same kinesis records... so you'd better monitor for errors.
  if (fatalErr) {
    logger.error(fatalErr);
    return callback(new Error(fatalErr));
  }

  // complain very loudly about unrecognized input records
  let inputs = new Inputs(records, process.env.PINGBACKS);
  inputs.unrecognized.forEach(r => {
    logger.warn(`Unrecognized input record: ${JSON.stringify(r)}`);
  });

  // run inserts in parallel
  inputs.insertAll().then(
    results => {
      let total = results.reduce((acc, r) => acc + r.count, 0);
      callback(null, `Inserted ${total} rows`);
    },
    err => {
      callback(logger.errors(err));
    }
  );
};
