'use strict';

const logger = require('./lib/logger');
const Inputs = require('./lib/inputs');

exports.handler = (event, context, callback) => {
  let records = [];

  // decode the base64 kinesis records
  if (!event || !event.Records) {
    logger.error(`Invalid event input: ${JSON.stringify(event)}`);
  } else {
    records = event.Records.map(r => {
      try {
        return JSON.parse(new Buffer(r.kinesis.data, 'base64').toString('utf-8'));
      } catch (decodeErr) {
        logger.error(`Invalid record input: ${JSON.stringify(r)}`);
        return null;
      }
    }).filter(f => f);
  }

  // nothing to do
  if (records.length === 0) {
    return callback();
  }

  // complain very loudly about unrecognized input records
  let inputs = new Inputs(records, process.env.PINGBACKS);
  inputs.unrecognized.forEach(r => {
    logger.error(`Unrecognized input record: ${JSON.stringify(r)}`);
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
