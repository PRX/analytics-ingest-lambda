'use strict';

// bigquery settings
beforeEach(() => {
  process.env.BQ_CLIENT_EMAIL = 'foo@bar.gov';
  process.env.BQ_PRIVATE_KEY = '-----BEGIN PRIVATE KEY-----foobar-----END PRIVATE KEY-----\n';
  process.env.BQ_PROJECT_ID = 'foobar_project';
  process.env.BQ_DATASET = 'foobar_dataset';
  process.env.BQ_DOVETAIL_TABLE = 'foobar_table';
});

// global includes
global.expect = require('chai').expect;
global.sinon = require('sinon');
