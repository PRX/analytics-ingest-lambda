'use strict';

const support = require('./support');
const RedisIncrements = require('../lib/inputs/redis-increments');

describe('redis-increments', () => {

  beforeEach(() => process.env.REDIS_HOST = 'redis://127.0.0.1:6379');

  it('recognizes download records', () => {
    let incr = new RedisIncrements();
    expect(incr.check({})).to.be.false;
    expect(incr.check({type: 'download', feederPodcast: 1, isDuplicate: false})).to.be.false;
    expect(incr.check({type: 'combined', feederPodcast: 1})).to.be.false;
    expect(incr.check({type: 'combined', feederPodcast: 1, download: {isDuplicate: true}})).to.be.false;
    expect(incr.check({type: 'combined', feederPodcast: 1, download: {isDuplicate: false}})).to.be.true;
    expect(incr.check({type: 'postbytes', feederPodcast: 1, download: {isDuplicate: true}})).to.be.false;
    expect(incr.check({type: 'postbytes', feederPodcast: 1, download: {}})).to.be.true;
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
      record(1490827132, 'combined', null, null)
    ]);
    return incr.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts nothing for no redis host', () => {
    process.env.REDIS_HOST = '';
    let records = [record(1490827132, 'combined', 1234, 'abcd')];
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

  it('does not increment duplicate records', async () => {
    let incr = new RedisIncrements([
      {...record(1490827132, 'combined', 1234, 'abcd'), remoteAgent: 'googlebot'}
    ]);
    expect(incr._records.length).to.equal(1);
    let result = await incr.insert();
    expect(result.length).to.equal(1); // TODO: bot-filter preview
  });

  it('increments redis counts', () => {
    let incr = new RedisIncrements([
      record(1490827132, 'download', 1234, 'abcd'),
      record(1490827132, 'combined', 1234, 'abcd'),
      record(1490827132, 'combined', 1234, 'efgh'),
      record(1490828132, 'combined', 1234, 'abcd'),
      record(1490829132, 'combined', 1234, 'abcd'),
      record(1490827132, 'impression', 1234, null),
      record(1490827132, 'impression', 1234, 'efgh', true)
    ]);
    return incr.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('redis://127.0.0.1');
      expect(result[0].count).to.equal(4 * 4); // skips impressions
      return support.redisGetAll('downloads.podcasts.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(3);
      expect(vals['downloads.podcasts.HOUR.2017-03-29T22:00:00Z']['1234']).to.equal('3');
      expect(vals['downloads.podcasts.HOUR.2017-03-29T23:00:00Z']['1234']).to.equal('1');
      expect(vals['downloads.podcasts.DAY.2017-03-29T00:00:00Z']['1234']).to.equal('4');
      return support.redisGetAll('impressions.podcasts.*');
    }).then(vals => {
      expect(Object.keys(vals).length).to.equal(0);
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
    let incr = new RedisIncrements([record(1490827132, 'combined', 1234, 'abcd')]);
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
  if (type === 'combined') {
    return {
      timestamp,
      type,
      feederPodcast: id,
      feederEpisode: guid,
      download: {isDuplicate: !!isdup}
    };
  } else {
    return {
      timestamp,
      type,
      feederPodcast: id,
      feederEpisode: guid,
      isDuplicate: !!isdup
    };
  }
}
