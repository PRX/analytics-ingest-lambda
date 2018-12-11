'use strict';

const support = require('./support');
const bigquery = require('../lib/bigquery');
const DovetailImpressions = require('../lib/inputs/dovetail-impressions');

describe('dovetail-impressions', () => {

  let impression = new DovetailImpressions();

  it('recognizes impression records', () => {
    expect(impression.check({type: null})).to.be.false;
    expect(impression.check({type: undefined})).to.be.false;
    expect(impression.check({type: 'download'})).to.be.false;
    expect(impression.check({type: 'impression'})).to.be.true;
    expect(impression.check({type: 'combined', impressions: []})).to.be.false;
    expect(impression.check({type: 'combined', impressions: [{}]})).to.be.true;
  });

  it('formats combined table inserts', async () => {
    const record = await impression.format({
      type: 'combined',
      timestamp: 1490827132999,
      impressions: [{adId: 1, isDuplicate: true}, {adId: 2, isDuplicate: false}],
      listenerEpisode: 'something'
    });
    expect(record.length).to.equal(2);

    expect(record[0]).to.have.keys('insertId', 'json');
    expect(record[0].insertId.length).to.be.above(10);
    expect(record[0].json).to.have.keys(
      'timestamp', 'feeder_podcast', 'feeder_episode',
      'listener_id', 'listener_episode', 'listener_session',
      'confirmed', 'segment',
      'ad_id', 'campaign_id', 'creative_id', 'flight_id',
      'is_duplicate', 'cause');
    expect(record[0].json.timestamp).to.equal(1490827132);
    expect(record[0].json.listener_episode).to.equal('something');
    expect(record[0].json.is_duplicate).to.equal(true);

    expect(record[1].json.timestamp).to.equal(1490827132);
    expect(record[1].json.listener_episode).to.equal('something');
    expect(record[1].json.is_duplicate).to.equal(false);
    expect(record[1].insertId).not.to.equal(record[0].insertId);
  });

  it('formats legacy table inserts', async () => {
    const record = await impression.format({
      type: 'impression',
      timestamp: 1490827132999,
      requestUuid: 'the-uuid',
      isDuplicate: true,
      adId: 1
    });
    expect(record).to.have.keys('insertId', 'json');
    expect(record.insertId).not.to.equal('the-uuid');
    expect(record.json).to.have.keys(
      'timestamp', 'request_uuid', 'feeder_podcast', 'feeder_episode',
      'is_duplicate', 'cause',
      'ad_id', 'campaign_id', 'creative_id', 'flight_id');
    expect(record.json.timestamp).to.equal(1490827132);
    expect(record.json.request_uuid).to.equal('the-uuid');
  });

  it('creates unique insert ids for ads', async () => {
    const r1 = await impression.format({requestUuid: 'req1', adId: 1});
    const r2 = await impression.format({requestUuid: 'req1', adId: 1});
    const r3 = await impression.format({requestUuid: 'req1', adId: 2});
    const r4 = await impression.format({requestUuid: 'req2', adId: 1});
    const [r5, r6, r7] = await impression.format({
      type: 'combined',
      listenerEpisode: 'something',
      impressions: [{adId: 1}, {adId: 1}, {adId: 2}]
    });
    expect(r1.insertId).to.equal(r2.insertId);
    expect(r1.insertId).not.to.equal(r3.insertId);
    expect(r1.insertId).not.to.equal(r4.insertId);
    expect(r5.insertId).to.equal(r6.insertId);
    expect(r5.insertId).not.to.equal(r7.insertId);
  });

  it('inserts nothing', () => {
    return impression.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts impression records', () => {
    let inserts = {};
    sinon.stub(bigquery, 'insert', (tbl, rows) => {
      inserts[tbl] = rows;
      return Promise.resolve(rows.length);
    });
    let impression2 = new DovetailImpressions([
      {type: 'impression', requestUuid: 'the-uuid1', timestamp: 1490827132999},
      {type: 'download', requestUuid: 'the-uuid2', timestamp: 1490827132999},
      {type: 'impression', requestUuid: 'the-uuid3', timestamp: 1490837132},
      {type: 'impression', requestUuid: 'the-uuid4', timestamp: 1490827132999},
      {type: 'combined', listenerEpisode: 'listen1', timestamp: 1490837132, impressions: []},
      {type: 'combined', listenerEpisode: 'listen2', timestamp: 1490827132999, impressions: [{adId: 1}, {adId: 2}]}
    ]);
    return impression2.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('dt_impressions');
      expect(result[0].count).to.equal(5);
      expect(inserts['dt_impressions'][0].json.request_uuid).to.equal('the-uuid1');
      expect(inserts['dt_impressions'][1].json.request_uuid).to.equal('the-uuid3');
      expect(inserts['dt_impressions'][2].json.request_uuid).to.equal('the-uuid4');
      expect(inserts['dt_impressions'][3].json.listener_episode).to.equal('listen2');
      expect(inserts['dt_impressions'][3].json.ad_id).to.equal(1);
      expect(inserts['dt_impressions'][4].json.listener_episode).to.equal('listen2');
      expect(inserts['dt_impressions'][4].json.ad_id).to.equal(2);
    });
  });

});
