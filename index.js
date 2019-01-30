'use strict';

const logger = require('./lib/logger');
const loadenv = require('./lib/loadenv');
const { BigqueryInputs, PingbackInputs, RedisInputs } = require('./lib/inputs');

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
    }).filter(r => r);
  }

  // nothing to do
  if (records.length === 0) {
    return callback();
  }

  loadenv.load(() => {
    // figure out what type of records we process
    let inputs;
    if (process.env.REDIS_HOST) {
      inputs = new RedisInputs(records);
    } else if (process.env.PINGBACKS) {
      inputs = new PingbackInputs(records);
    } else {
      inputs = new BigqueryInputs(records);
    }

    // complain very loudly about unrecognized input records
    inputs.unrecognized.forEach(r => {
      logger.error(`Unrecognized input record: ${JSON.stringify(r)}`);
    });

    // failsafe timeout on pingbacks
    let failsafe;
    if (process.env.PINGBACKS) {
      failsafe = setTimeout(function() {
        logger.error('Failsafe global timeout!');
        console.log(JSON.stringify(records));
        if (process.env.EXIT_ON_PINGBACKS_TIMEOUT) {
          callback(null, `Failsafe exit`);
          process.exit(0);
        }
      }, 3000);
    }

    // run inserts in parallel
    inputs.insertAll().then(
      results => {
        clearTimeout(failsafe);
        let total = results.reduce((acc, r) => acc + r.count, 0);
        callback(null, `Inserted ${total} rows`);
      },
      err => {
        clearTimeout(failsafe);
        callback(logger.errors(err));
      }
    );
  });
};
