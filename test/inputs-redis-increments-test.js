'use strict';

const support = require('./support');
const redis = require('../lib/redis');
const RedisIncrements = require('../lib/inputs/redis-increments');

describe('redis-increments', () => {

  beforeEach(() => process.env.REDIS_HOST = 'redis://127.0.0.1:6379');

  it('recognizes download and impression records', () => {
    let incr = new RedisIncrements();
    expect(incr.check({})).to.be.false;
    expect(incr.check({type: 'foobar'})).to.be.false;
    expect(incr.check({type: 'download'})).to.be.true;
    expect(incr.check({type: 'impression'})).to.be.true;
    expect(incr.check({type: 'download', isDuplicate: true})).to.be.false;
    expect(incr.check({type: 'download', isDuplicate: false})).to.be.true;
  });

  it('inserts nothing', () => {
    let incr = new RedisIncrements();
    return incr.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('increments redis counts', () => {
    let incr = new RedisIncrements([
      record(1490827132, 'download', 1234, 'abcd'),
      record(1490827132, 'download', 1234, 'efgh'),
      record(1490828132, 'download', 1234, 'abcd'),
      record(1490827132, 'impression', 1234, null),
      record(1490827132, 'impression', 1234, 'efgh', true)
    ]);
    return incr.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('redis');
      expect(result[0].count).to.equal(4 * 5 + 3 * 5); // skips null episode impression
      return support.redisGetAll('downloads.podcasts.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(6);
      expect(vals['downloads.podcasts.15MIN.2017-03-29T22:30:00Z']['1234']).to.equal('2');
      expect(vals['downloads.podcasts.15MIN.2017-03-29T22:45:00Z']['1234']).to.equal('1');
      expect(vals['downloads.podcasts.HOUR.2017-03-29T22:00:00Z']['1234']).to.equal('3');
      expect(vals['downloads.podcasts.DAY.2017-03-29T00:00:00Z']['1234']).to.equal('3');
      expect(vals['downloads.podcasts.WEEK.2017-03-26T00:00:00Z']['1234']).to.equal('3');
      expect(vals['downloads.podcasts.MONTH.2017-03-01T00:00:00Z']['1234']).to.equal('3');
      return support.redisGetAll('impressions.podcasts.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(5);
      expect(vals['impressions.podcasts.15MIN.2017-03-29T22:30:00Z']['1234']).to.equal('1');
      expect(vals['impressions.podcasts.HOUR.2017-03-29T22:00:00Z']['1234']).to.equal('1');
      expect(vals['impressions.podcasts.DAY.2017-03-29T00:00:00Z']['1234']).to.equal('1');
      expect(vals['impressions.podcasts.WEEK.2017-03-26T00:00:00Z']['1234']).to.equal('1');
      expect(vals['impressions.podcasts.MONTH.2017-03-01T00:00:00Z']['1234']).to.equal('1');
      return support.redisGetAll('downloads.episodes.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(6);
      expect(vals['downloads.episodes.15MIN.2017-03-29T22:30:00Z']['abcd']).to.equal('1');
      expect(vals['downloads.episodes.15MIN.2017-03-29T22:30:00Z']['efgh']).to.equal('1');
      expect(vals['downloads.episodes.15MIN.2017-03-29T22:45:00Z']['abcd']).to.equal('1');
      expect(vals['downloads.episodes.HOUR.2017-03-29T22:00:00Z']['abcd']).to.equal('2');
      expect(vals['downloads.episodes.HOUR.2017-03-29T22:00:00Z']['efgh']).to.equal('1');
      expect(vals['downloads.episodes.DAY.2017-03-29T00:00:00Z']['abcd']).to.equal('2');
      expect(vals['downloads.episodes.DAY.2017-03-29T00:00:00Z']['efgh']).to.equal('1');
      expect(vals['downloads.episodes.WEEK.2017-03-26T00:00:00Z']['abcd']).to.equal('2');
      expect(vals['downloads.episodes.WEEK.2017-03-26T00:00:00Z']['efgh']).to.equal('1');
      expect(vals['downloads.episodes.MONTH.2017-03-01T00:00:00Z']['abcd']).to.equal('2');
      expect(vals['downloads.episodes.MONTH.2017-03-01T00:00:00Z']['efgh']).to.equal('1');
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
      expect(result[0].dest).to.equal('redis');
      expect(result[0].count).to.equal(2 * 5);
      return support.redisTTLAll('downloads.*');
    }).then(ttlMap => {
      expect(Object.keys(ttlMap).length).to.equal(10);
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
