'use strict';

const support = require('./support');
const Redis = require('../lib/redis');

describe('redis', () => {

  let redis = new Redis('redis:127.0.0.1:6379');
  beforeEach(() => redis.connect());
  afterEach(() => redis.disconnect());

  it('generates date keys', () => {
    let keys = Redis.keys(new Date('2017-10-26T17:26:28Z'));
    expect(keys.length).to.equal(5);
    expect(keys[0]).to.equal('15MIN.2017-10-26T17:15:00Z');
    expect(keys[1]).to.equal('HOUR.2017-10-26T17:00:00Z');
    expect(keys[2]).to.equal('DAY.2017-10-26T00:00:00Z');
    expect(keys[3]).to.equal('WEEK.2017-10-22T00:00:00Z');
    expect(keys[4]).to.equal('MONTH.2017-10-01T00:00:00Z');
  });

  it('gets the beginning of week correctly', () => {
    let key = Redis.keys(new Date('2017-10-22T00:00:01Z'))[3];
    expect(key).to.equal('WEEK.2017-10-22T00:00:00Z');
    key = Redis.keys(new Date('2017-10-22T00:00:00Z'))[3];
    expect(key).to.equal('WEEK.2017-10-22T00:00:00Z');
    key = Redis.keys(new Date('2017-10-21T23:59:59Z'))[3];
    expect(key).to.equal('WEEK.2017-10-15T00:00:00Z');
  });

  it('gets the beginning of month correctly', () => {
    let key = Redis.keys(new Date('2017-10-01T00:00:01Z'))[4];
    expect(key).to.equal('MONTH.2017-10-01T00:00:00Z');
    key = Redis.keys(new Date('2017-10-01T00:00:00Z'))[4];
    expect(key).to.equal('MONTH.2017-10-01T00:00:00Z');
    key = Redis.keys(new Date('2017-09-30T23:59:59Z'))[4];
    expect(key).to.equal('MONTH.2017-09-01T00:00:00Z');
  });

  it('gets scoped keys', () => {
    let keys = Redis.podcastDownloads(new Date('2017-10-01T00:00:01Z'));
    keys.forEach(key => expect(key).to.match(/^downloads\.podcasts\./));
    keys = Redis.podcastImpressions(new Date('2017-10-01T00:00:01Z'));
    keys.forEach(key => expect(key).to.match(/^impressions\.podcasts\./));
    keys = Redis.episodeDownloads(new Date('2017-10-01T00:00:01Z'));
    keys.forEach(key => expect(key).to.match(/^downloads\.episodes\./));
    keys = Redis.episodeImpressions(new Date('2017-10-01T00:00:01Z'));
    keys.forEach(key => expect(key).to.match(/^impressions\.episodes\./));
  });

  it('handles lack of configuration', () => {
    let nullRedis = new Redis(null, null);
    expect(nullRedis.hostName()).to.be.null;
  });

  it('increments hashes', () => {
    return redis.increment('foo', 'bar', 2).then(reply => {
      expect(reply).to.equal(2);
      return redis.increment('foo', 'bar', 3);
    }).then(reply => {
      expect(reply).to.equal(5);
      return support.redisHget('foo', 'bar');
    }).then(reply => {
      expect(reply).to.equal(5);
    });
  });

  it('expires keys', () => {
    return redis.increment('foo', 'bar', 1).then(reply => {
      return support.redisTTL('foo');
    }).then(reply => {
      expect(reply).to.equal(-1);
      return redis.expire('foo');
    }).then(reply => {
      expect(reply).to.equal(1);
      return support.redisTTL('foo');
    }).then(reply => {
      expect(reply).to.be.above(30);
    });
  });

});