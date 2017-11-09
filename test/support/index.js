'use strict';

// bigquery settings
beforeEach(() => {
  process.env.BQ_CLIENT_EMAIL = 'foo@bar.gov';
  process.env.BQ_PRIVATE_KEY = '-----BEGIN PRIVATE KEY-----foobar-----END PRIVATE KEY-----\n';
  process.env.BQ_PROJECT_ID = 'foobar_project';
  process.env.BQ_DATASET = 'foobar_dataset';
  process.env.BQ_DOWNLOADS_TABLE = 'the_downloads_table';
  process.env.BQ_IMPRESSIONS_TABLE = 'the_impressions_table';
  process.env.REDIS_HOST = '';
  process.env.REDIS_TTL = '7200';
});

// global includes
global.chai = require('chai');
global.chai.use(require('sinon-chai'));
global.expect = chai.expect;
global.nock = require('nock');

// sandbox sinon mocks
const _sinon = require('sinon');
beforeEach(() => global.sinon = _sinon.sandbox.create());
afterEach(() => global.sinon.restore());

// fake redis - TODO: lib doesn't handle connect/quit correctly
const redis = require('redis');
const fakeRedis = require('fakeredis');
let fakeClient;
beforeEach(() => {
  fakeClient = fakeRedis.createClient({fast: true});
  let _on = fakeClient.on, _quit = fakeClient.quit;
  fakeClient.on = (e, fn) => (e === 'connect') ? fn() : _on.call(fakeClient, e, fn);
  fakeClient.quit = () => fakeClient.emit('end') && _quit.call(fakeClient);
  sinon.stub(redis, 'createClient', () => fakeClient);
});
afterEach(() => exports.redisCommand('flushdb'));

// redis helpers
exports.redisCommand = function() {
  let cmd = arguments[0], args = [].slice.call(arguments, 1);
  let client = fakeRedis.createClient({fast: true});
  return new Promise((resolve, reject) => {
    client[cmd].call(client, args, (err, reply) => err ? reject(err) : resolve(reply));
  });
}
exports.redisKeys = pattern => exports.redisCommand('keys', pattern);
exports.redisHget = (key, field) => exports.redisCommand('hget', key, field);
exports.redisTTL = key => exports.redisCommand('ttl', key);
exports.redisGetAll = pattern => {
  return exports.redisKeys(pattern).then(keys => {
    let mapAll = {};
    return Promise.all(keys.map(key => {
      return exports.redisCommand('hgetall', key).then(all => mapAll[key] = all);
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
