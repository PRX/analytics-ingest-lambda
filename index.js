'use strict';

const zlib = require('zlib');
const { promisify } = require('util');
const logger = require('./lib/logger');
const loadenv = require('./lib/loadenv');
const timestamp = require('./lib/timestamp');
const { BigqueryInputs, DynamoInputs, PingbackInputs, RedisInputs } = require('./lib/inputs');

exports.handler = async (event) => {
  let records = [];

  // debug timeouts
  let timer = null;
  if (process.env.DEBUG) {
    timer = setTimeout(() => logger.error('TIMEOUT', {event}), 29000);
  }

  // decode the base64 kinesis records
  if (!event || !event.Records) {
    logger.error(`Invalid event input: ${JSON.stringify(event)}`);
  } else {
    // concat in the log filter case: an event record contains a set of records.
    records = [].concat.apply([], await Promise.all(event.Records.map(async r => {
      try {
        return JSON.parse(Buffer.from(r.kinesis.data, 'base64').toString('utf-8'));
      } catch (decodeErr) {
          // In the case that our kinesis data is base64 + gzipped,
          // it is coming from the dovetail-router log filter subscription.
          try{
              const buffer = Buffer.from(r.kinesis.data, 'base64');
              const unzipped = await promisify(zlib.gunzip)(buffer)
              return JSON.parse(unzipped).logEvents.map(logLine => JSON.parse(logLine.message));
          }
          catch (decodeErr){
            logger.error(`Invalid record input: ${decodeErr} ${JSON.stringify(r)}`);
            return null;
          }
      }
    }))).filter(r => {
      if (process.env.PROCESS_AFTER && r && r.timestamp) {
        const after = timestamp.toEpochSeconds(parseInt(process.env.PROCESS_AFTER));
        const time = timestamp.toEpochSeconds(r.timestamp);
        return time > after;
      } else if (process.env.PROCESS_UNTIL && r && r.timestamp) {
        const until = timestamp.toEpochSeconds(parseInt(process.env.PROCESS_UNTIL));
        const time = timestamp.toEpochSeconds(r.timestamp);
        return time <= until;
      } else {
        return !!r;
      }
    });
  }

  // nothing to do
  if (records.length === 0) {
    clearTimeout(timer);
    return;
  }

  await new Promise((resolve, reject) => {
    loadenv.load(() => {
      resolve()
    })
  })

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
  try{
    const results = await inputs.insertAll();
    clearTimeout(timer);
    let total = results.reduce((acc, r) => acc + r.count, 0);
    return `Inserted ${total} rows`;
  } catch(err) {
      clearTimeout(timer);
      return logger.errors(err);
  }
};
