'use strict';

// bigquery settings
beforeEach(() => {
  process.env.BQ_CLIENT_EMAIL = 'foo@bar.gov';
  process.env.BQ_PRIVATE_KEY = '-----BEGIN PRIVATE KEY-----foobar-----END PRIVATE KEY-----\n';
  process.env.BQ_PROJECT_ID = 'foobar_project';
  process.env.BQ_DATASET = 'foobar_dataset';
  process.env.DDB_TABLE = 'foobar_table';
  process.env.DDB_ROLE = '';
  process.env.DDB_TTL = '';
  process.env.DYNAMODB = '';
  process.env.KINESIS_STREAM = 'foobar_stream';
  process.env.KINESIS_RETRY_STREAM = 'foobar_retry_stream';
  process.env.PINGBACKS = '';
});

// global includes
global.chai = require('chai');
global.chai.use(require('sinon-chai'));
global.expect = chai.expect;
global.nock = require('nock');

// sandbox sinon mocks
const _sinon = require('sinon');
beforeEach(() => (global.sinon = _sinon.createSandbox()));
afterEach(() => global.sinon.restore());

exports.buildRecord = require('./build').buildRecord;
exports.buildEvent = require('./build').buildEvent;
