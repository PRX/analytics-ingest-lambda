'use strict';

if (!process.env.BQ_CLIENT_EMAIL) {
  require('dotenv').config();
}
const testEvent = require('../support/test-event');
const index     = require('../../index');
const handler   = index.handler;

// decode pingback settings
if (process.env.REDIS_HOST && process.env.REDIS_HOST !== '0') {
  delete process.env.PINGBACKS;
  console.log('Running REDIS');
} else if (process.env.PINGBACKS && process.env.PINGBACKS !== '0') {
  delete process.env.REDIS_HOST;
  console.log('Running PINGBACKS');
} else {
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
