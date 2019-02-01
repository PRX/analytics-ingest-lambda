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
    expect(impression.check({type: 'impression'})).to.be.false;
    expect(impression.check({type: 'combined', impressions: []})).to.be.false;
    expect(impression.check({type: 'combined', impressions: [{}]})).to.be.true;
  });

  it('formats table inserts', async () => {
    const record = await impression.format({
      type: 'combined',
      timestamp: 1490827132999,
      impressions: [{adId: 1, isDuplicate: true}, {adId: 2, isDuplicate: false}],
      listenerSession: 'something'
    });
    expect(record.length).to.equal(2);

    expect(record[0]).to.have.keys('insertId', 'json');
    expect(record[0].insertId.length).to.be.above(10);
    expect(record[0].json).to.have.keys(
      'timestamp', 'request_uuid', 'feeder_podcast', 'feeder_episode',
      'digest', 'listener_session',
      'is_confirmed', 'is_bytes', 'segment',
      'ad_id', 'campaign_id', 'creative_id', 'flight_id',
      'is_duplicate', 'cause');
    expect(record[0].json.timestamp).to.equal(1490827132);
    expect(record[0].json.listener_session).to.equal('something');
    expect(record[0].json.is_duplicate).to.equal(true);

    expect(record[1].json.timestamp).to.equal(1490827132);
    expect(record[1].json.listener_session).to.equal('something');
    expect(record[1].json.is_duplicate).to.equal(false);
    expect(record[1].insertId).not.to.equal(record[0].insertId);
  });

  it('creates unique insert ids for ads', async () => {
    const r1 = await impression.format({impressions: [{adId: 1}], listenerSession: 'req1'});
    const r2 = await impression.format({impressions: [{adId: 1}], listenerSession: 'req1'});
    const r3 = await impression.format({impressions: [{adId: 2}], listenerSession: 'req1'});
    const r4 = await impression.format({impressions: [{adId: 1}], listenerSession: 'req2'});
    expect(r1[0].insertId).to.equal(r2[0].insertId);
    expect(r1[0].insertId).not.to.equal(r3[0].insertId);
    expect(r1[0].insertId).not.to.equal(r4[0].insertId);
  });

  it('inserts nothing', () => {
    return impression.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts impression records', () => {
    let inserts = {};
    sinon.stub(bigquery, 'insert').callsFake((tbl, rows) => {
      inserts[tbl] = rows;
      return Promise.resolve(rows.length);
    });
    let impression2 = new DovetailImpressions([
      {type: 'impression', requestUuid: 'the-uuid1', timestamp: 1490827132999},
      {type: 'download', requestUuid: 'the-uuid2', timestamp: 1490827132999},
      {type: 'combined', listenerSession: 'listen1', timestamp: 1490837132, impressions: []},
      {type: 'combined', listenerSession: 'listen2', timestamp: 1490827132999, impressions: [{adId: 1}, {adId: 2}]},
      {type: 'combined', listenerSession: 'listen3', timestamp: 1490837132, impressions: [{isDuplicate: true, adId: 3}]}
    ]);
    return impression2.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('dt_impressions');
      expect(result[0].count).to.equal(3);
      expect(inserts['dt_impressions'][0].json.listener_session).to.equal('listen2');
      expect(inserts['dt_impressions'][0].json.ad_id).to.equal(1);
      expect(inserts['dt_impressions'][1].json.listener_session).to.equal('listen2');
      expect(inserts['dt_impressions'][1].json.ad_id).to.equal(2);
      expect(inserts['dt_impressions'][2].json.listener_session).to.equal('listen3');
      expect(inserts['dt_impressions'][2].json.ad_id).to.equal(3);
    });
  });

});
