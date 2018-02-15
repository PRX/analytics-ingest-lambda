'use strict';

const support = require('./support');
const redis = require('../lib/redis');
const RedisIncrements = require('../lib/inputs/redis-increments');

describe('redis-increments', () => {

  beforeEach(() => process.env.REDIS_HOST = 'redis://127.0.0.1:6379');

  it('recognizes download and impression records', () => {
    let incr = new RedisIncrements();
    expect(incr.check({})).to.be.false;
    expect(incr.check({type: 'foobar', feederPodcast: 1})).to.be.false;
    expect(incr.check({type: 'download', feederPodcast: 1})).to.be.true;
    expect(incr.check({type: 'impression', feederPodcast: 1})).to.be.true;
    expect(incr.check({type: 'download', feederPodcast: 1, isDuplicate: true})).to.be.false;
    expect(incr.check({type: 'download', feederPodcast: 1, isDuplicate: false})).to.be.true;
  });

  it('inserts nothing for no records', () => {
    let incr = new RedisIncrements();
    return incr.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts nothing for records without feeder ids/guids', () => {
    let incr = new RedisIncrements([
      record(1490827132, 'download', null, null),
      record(1490827132, 'impression', null, null)
    ]);
    return incr.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts nothing for no redis host', () => {
    process.env.REDIS_HOST = '';
    let records = [record(1490827132, 'download', 1234, 'abcd')];
    let incr = new RedisIncrements(records);
    return incr.insert().then(result => {
      expect(result.length).to.equal(0);

      process.env.REDIS_HOST = 'whatev';
      incr = new RedisIncrements(records);
      return incr.insert();
    }).then(result => {
      expect(result.length).to.equal(1);
    });
  });

  it('increments redis counts', () => {
    let incr = new RedisIncrements([
      record(1490827132, 'download', 1234, 'abcd'),
      record(1490827132, 'download', 1234, 'efgh'),
      record(1490828132, 'download', 1234, 'abcd'),
      record(1490829132, 'download', 1234, 'abcd'),
      record(1490827132, 'impression', 1234, null),
      record(1490827132, 'impression', 1234, 'efgh', true)
    ]);
    return incr.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('redis://127.0.0.1');
      expect(result[0].count).to.equal(4 * 4 + 1 * 2); // skips null episode impression
      return support.redisGetAll('downloads.podcasts.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(3);
      expect(vals['downloads.podcasts.HOUR.2017-03-29T22:00:00Z']['1234']).to.equal('3');
      expect(vals['downloads.podcasts.HOUR.2017-03-29T23:00:00Z']['1234']).to.equal('1');
      expect(vals['downloads.podcasts.DAY.2017-03-29T00:00:00Z']['1234']).to.equal('4');
      return support.redisGetAll('impressions.podcasts.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(2);
      expect(vals['impressions.podcasts.HOUR.2017-03-29T22:00:00Z']['1234']).to.equal('1');
      expect(vals['impressions.podcasts.DAY.2017-03-29T00:00:00Z']['1234']).to.equal('1');
      return support.redisGetAll('downloads.episodes.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(3);
      expect(vals['downloads.episodes.HOUR.2017-03-29T22:00:00Z']['abcd']).to.equal('2');
      expect(vals['downloads.episodes.HOUR.2017-03-29T22:00:00Z']['efgh']).to.equal('1');
      expect(vals['downloads.episodes.HOUR.2017-03-29T23:00:00Z']['abcd']).to.equal('1');
      expect(vals['downloads.episodes.DAY.2017-03-29T00:00:00Z']['abcd']).to.equal('3');
      expect(vals['downloads.episodes.DAY.2017-03-29T00:00:00Z']['efgh']).to.equal('1');
      return support.redisGetAll('impressions.episodes.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(0);
    });
  });

  it('expires redis keys', () => {
    let incr = new RedisIncrements([
      record(1490827132, 'download', 1234, 'abcd')
    ]);
    return incr.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('redis://127.0.0.1');
      expect(result[0].count).to.equal(2 * 2);
      return support.redisTTLAll('downloads.*');
    }).then(ttlMap => {
      expect(Object.keys(ttlMap).length).to.equal(4);
      Object.keys(ttlMap).forEach(key => {
        expect(ttlMap[key]).to.equal(7200);
      });
    });
  });

});

// helpers
function record(timestamp, type, id, guid, isdup) {
  return {
    timestamp: timestamp,
    type: type,
    feederPodcast: id,
    feederEpisode: guid,
    isDuplicate: !!isdup
  }
}
