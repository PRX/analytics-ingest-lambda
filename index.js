'use strict';

const { getRecordsFromEvent } = require('./lib/get-records');
const logger = require('./lib/logger');
const loadenv = require('./lib/loadenv');
const timestamp = require('./lib/timestamp');
const { BigqueryInputs, DynamoInputs, PingbackInputs, FrequencyInputs } = require('./lib/inputs');

exports.handler = async event => {
  let records = [];

  // debug timeouts
  let timer = null;
  if (process.env.DEBUG) {
    timer = setTimeout(() => logger.error('TIMEOUT', { event }), 29000);
  }

  if (!event || !event.Records) {
    logger.error(`Invalid event input: ${JSON.stringify(event)}`);
  } else {
    records = (await getRecordsFromEvent(event)).filter(r => {
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

  // log the raw/decoded input counts
  logger.info('Event records', { raw: event.Records.length, decoded: records.length });

  await new Promise((resolve, reject) => {
    loadenv.load(() => {
      resolve();
    });
  });

  // figure out what type of records we process
  let inputs;
  if (process.env.PINGBACKS) {
    inputs = new PingbackInputs(records);
  } else if (process.env.DYNAMODB) {
    inputs = new DynamoInputs(records);
  } else if (process.env.FREQUENCY) {
    inputs = new FrequencyInputs(records);
  } else {
    inputs = new BigqueryInputs(records);
  }

  // complain very loudly about unrecognized input records
  inputs.unrecognized.forEach(r => {
    logger.error(`Unrecognized input record: ${JSON.stringify(r)}`);
  });

  // run inserts in parallel
  try {
    const results = await inputs.insertAll();
    clearTimeout(timer);
    let total = results.reduce((acc, r) => acc + r.count, 0);
    return `Inserted ${total} rows`;
  } catch (err) {
    clearTimeout(timer);
    throw logger.errors(err);
  }
};
