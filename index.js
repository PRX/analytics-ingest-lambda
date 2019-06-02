'use strict';

const logger = require('./lib/logger');
const loadenv = require('./lib/loadenv');
const { BigqueryInputs, DynamoInputs, PingbackInputs, RedisInputs } = require('./lib/inputs');

exports.handler = (event, context, callback) => {
  let records = [];

  // debug timeouts
  let timer = null;
  if (process.env.DEBUG) {
    timer = setTimeout(function() {
      console.log('[TIMEOUT]');
      console.log(JSON.stringify(event, null, 2));
    }, 29000);
  }

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
    clearTimeout(timer);
    return callback();
  }

  loadenv.load(() => {
    // figure out what type of records we process
    let inputs;
    if (process.env.REDIS_HOST) {
      inputs = new RedisInputs(records);
    } else if (process.env.PINGBACKS) {
      inputs = new PingbackInputs(records);
    } else if (process.env.DYNAMODB) {
      inputs = new DynamoInputs(records);
    } else {
      inputs = new BigqueryInputs(records);
    }

    // complain very loudly about unrecognized input records
    inputs.unrecognized.forEach(r => {
      logger.error(`Unrecognized input record: ${JSON.stringify(r)}`);
    });

    // run inserts in parallel
    inputs.insertAll().then(
      results => {
        clearTimeout(timer);
        let total = results.reduce((acc, r) => acc + r.count, 0);
        callback(null, `Inserted ${total} rows`);
      },
      err => {
        clearTimeout(timer);
        callback(logger.errors(err));
      }
    );
  });
};
