'use strict';

if (!process.env.BQ_CLIENT_EMAIL) {
  require('dotenv').config();
}
const testEvent = require('../support/test-event');
const index     = require('../../index');
const handler   = index.handler;

// decode pingback settings
if (!process.env.PINGBACKS || process.env.PINGBACKS === '0') {
  delete process.env.PINGBACKS;
  console.log('Running BIGQUERY');
} else {
  console.log('Running PINGBACKS');
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
