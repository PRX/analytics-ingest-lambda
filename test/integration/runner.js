'use strict';

if (!process.env.BQ_CLIENT_EMAIL) {
  require('dotenv').config();
}
const { buildMixedStyleEvent } = require('../support/build');
const handler = require('../../index').handler;

// bigquery does not like timestamps more than 7 days in the past
const testRecordSets = require('../support/test-runner-records');

const testInputStyleRecords = testRecordSets.inputStyleRecords.map(rec => {
  return {...rec, timestamp: new Date().getTime()};
}).filter(r => r.type !== 'foo')

const testEvent = buildMixedStyleEvent(testInputStyleRecords,
  testRecordSets.kinesisEventStyleRecords);

// decode pingback settings
if (process.env.REDIS_HOST && process.env.REDIS_HOST !== '0') {
  delete process.env.PINGBACKS;
  console.log('Running REDIS');
} else if (process.env.PINGBACKS && process.env.PINGBACKS !== '0') {
  delete process.env.REDIS_HOST;
  console.log('Running PINGBACKS');
} else if (process.env.DYNAMODB && process.env.DYNAMODB !== '0') {
  delete process.env.PINGBACKS;
  delete process.env.REDIS_HOST;
  console.log('Running DYNAMODB');
} else {
  delete process.env.DYNAMODB;
  delete process.env.PINGBACKS;
  delete process.env.REDIS_HOST;
  console.log('Running BIGQUERY');
}

/**
 * Run the test event for real, against your .env settings
 */
handler(testEvent, null, (err, result) => {
  if (err) {
    console.error('Exited with error!');
    console.error(err);
    process.exit(1);
  } else {
    console.log('Exited success:', result);
  }
});
