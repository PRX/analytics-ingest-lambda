'use strict';

if (!process.env.BQ_CLIENT_EMAIL) {
  require('dotenv').config();
}
const { buildMixedStyleEvent } = require('../support/build');
const handler = require('../../index').handler;
const pingurl = require('../../lib/pingurl');

// bigquery does not like timestamps more than 7 days in the past
const testRecordSets = require('../support/test-runner-records');

const testInputStyleRecords = testRecordSets.inputStyleRecords
  .map(rec => {
    return { ...rec, timestamp: new Date().getTime() };
  })
  .filter(r => r.type !== 'foo');

const testEvent = buildMixedStyleEvent(
  testInputStyleRecords,
  testRecordSets.kinesisEventStyleRecords,
);

// decode pingback settings
// todo: Add in frequency
if (process.env.PINGBACKS && process.env.PINGBACKS !== '0') {
  delete process.env.DYNAMODB;
  delete process.env.FREQUENCY;
  console.log('Running PINGBACKS');
} else if (process.env.DYNAMODB && process.env.DYNAMODB !== '0') {
  delete process.env.PINGBACKS;
  delete process.env.FREQUENCY;
  console.log('Running DYNAMODB');
} else if (process.env.FREQUENCY && process.env.FREQUENCY !== '0') {
  delete process.env.PINGBACKS;
  delete process.env.DYNAMODB;
  console.log('Running FREQUENCY');
} else {
  delete process.env.DYNAMODB;
  delete process.env.PINGBACKS;
  delete process.env.FREQUENCY;
  console.log('Running BIGQUERY');
}

/**
 * Run the test event for real, against your .env settings
 */
async function main() {
  try {
    const result = await handler(testEvent);
    console.log('\nExited success:', result);
  } catch (err) {
    console.error('\n\nExited with error!');
    console.error(err);
    console.error('\n');
    process.exit(1);
  }
}
main();
