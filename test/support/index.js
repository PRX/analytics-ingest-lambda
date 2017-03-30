'use strict';

// bigquery settings
beforeEach(() => {
  process.env.BQ_CLIENT_EMAIL = 'foo@bar.gov';
  process.env.BQ_PRIVATE_KEY = '-----BEGIN PRIVATE KEY-----foobar-----END PRIVATE KEY-----\n';
  process.env.BQ_PROJECT_ID = 'foobar_project';
  process.env.BQ_DATASET = 'foobar_dataset';
  process.env.BQ_DOWNLOADS_TABLE = 'the_downloads_table';
  process.env.BQ_IMPRESSIONS_TABLE = 'the_impressions_table';
});

// global includes
global.chai = require('chai');
global.chai.use(require('sinon-chai'));
global.expect = chai.expect;

// sandbox sinon mocks
const _sinon = require('sinon');
beforeEach(() => global.sinon = _sinon.sandbox.create());
afterEach(() => global.sinon.restore());

// build a base64-encoded kinesis record
exports.buildRecord = (record) => {
  return {
    eventSource: 'aws:kinesis',
    eventVersion: '1.0',
    kinesis: {
      data: new Buffer(JSON.stringify(record), 'utf-8').toString('base64')
    }
  };
};

// build an event of multiple kinesis records
exports.buildEvent = (records) => {
  return {
    Records: records.map(r => exports.buildRecord(r))
  };
};
