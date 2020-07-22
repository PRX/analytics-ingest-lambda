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
  process.env.REDIS_HOST = '';
  process.env.REDIS_TTL = '7200';
  process.env.REDIS_IMPRESSIONS_HOST = '';
  process.env.REDIS_IMPRESSIONS_TTL = '90000';
});

// global includes
global.chai = require('chai');
global.chai.use(require('sinon-chai'));
global.expect = chai.expect;
global.nock = require('nock');

// sandbox sinon mocks
const _sinon = require('sinon');
beforeEach(() => global.sinon = _sinon.createSandbox());
afterEach(() => global.sinon.restore());

// ioredis mock
const Redis = require('../../lib/redis');
const RedisMock = require('ioredis-mock');
let fakeClient;
beforeEach(() => {
  fakeClient = new RedisMock();
  sinon.stub(Redis, 'buildConn').returns(fakeClient);
});

// redis helpers
exports.redisKeys = pattern => fakeClient.keys(pattern);
exports.redisHget = (key, field) => fakeClient.hget(key, field);
exports.redisHgetAll = (key) => fakeClient.hgetall(key);
exports.redisTTL = key => fakeClient.ttl(key);
exports.redisGetAll = pattern => {
  return exports.redisKeys(pattern).then(keys => {
    let mapAll = {};
    return Promise.all(keys.map(key => {
      return exports.redisHgetAll(key).then(all => mapAll[key] = all);
    })).then(() => mapAll);
  });
}
exports.redisTTLAll = pattern => {
  return exports.redisKeys(pattern).then(keys => {
    let mapAll = {};
    return Promise.all(keys.map(key => {
      return exports.redisTTL(key).then(ttl => mapAll[key] = ttl);
    })).then(() => mapAll);
  });
}

exports.buildRecord = require('./build').buildRecord;
exports.buildEvent = require('./build').buildEvent;
