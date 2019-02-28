'use strict';

require('./support');
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
    expect(impression.check({type: 'postbytes', impressions: []})).to.be.false;
    expect(impression.check({type: 'postbytes', impressions: [{}]})).to.be.true;
    expect(impression.check({type: 'postbytespreview', impressions: []})).to.be.false;
    expect(impression.check({type: 'postbytespreview', impressions: [{}]})).to.be.true;
  });

  it('knows the table names of records', () => {
    expect(impression.tableName({type: 'combined'})).to.equal('dt_impressions');
    expect(impression.tableName({type: 'postbytes'})).to.equal('dt_impressions');
    expect(impression.tableName({type: 'postbytespreview'})).to.equal('dt_impressions_preview');
  });

  it('knows which records are bytes', () => {
    expect(impression.isBytes({type: 'combined'})).to.be.false;
    expect(impression.isBytes({type: 'postbytes'})).to.be.true;
    expect(impression.isBytes({type: 'postbytespreview'})).to.be.true;
  });

  it('formats table inserts', () => {
    const rec = {type: 'combined', timestamp: 1490827132999, listenerEpisode: 'something'};

    const format1 = impression.format(rec, {adId: 1, isDuplicate: true});
    expect(format1).to.have.keys('insertId', 'json');
    expect(format1.insertId.length).to.be.above(10);
    expect(format1.json).to.have.keys(
      'timestamp', 'request_uuid', 'feeder_podcast', 'feeder_episode',
      'digest', 'listener_session',
      'is_confirmed', 'is_bytes', 'segment',
      'ad_id', 'campaign_id', 'creative_id', 'flight_id',
      'is_duplicate', 'cause'
    );
    expect(format1.json.timestamp).to.equal(1490827132);
    expect(format1.json.listener_session.length).to.be.above(10);
    expect(format1.json.is_duplicate).to.equal(true);

    const format2 = impression.format(rec, {adId: 2, isDuplicate: false});
    expect(format2.json.timestamp).to.equal(1490827132);
    expect(format2.json.listener_session).to.equal(format1.json.listener_session);
    expect(format2.json.is_duplicate).to.equal(false);
    expect(format2.insertId).not.to.equal(format1.insertId);
  });

  it('creates unique insert ids for ads', () => {
    const r1 = impression.format({listenerEpisode: 'req1'}, {adId: 1});
    const r2 = impression.format({listenerEpisode: 'req1'}, {adId: 1});
    const r3 = impression.format({listenerEpisode: 'req1'}, {adId: 2});
    const r4 = impression.format({listenerEpisode: 'req2'}, {adId: 1});
    expect(r1.insertId).to.equal(r2.insertId);
    expect(r1.insertId).not.to.equal(r3.insertId);
    expect(r1.insertId).not.to.equal(r4.insertId);
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
      {type: 'combined', listenerEpisode: 'listen1', timestamp: 1490837132, impressions: []},
      {type: 'combined', listenerEpisode: 'listen2', timestamp: 1490827132999, impressions: [{adId: 1}, {adId: 2}]},
      {type: 'combined', listenerEpisode: 'listen3', timestamp: 1490837132, impressions: [{isDuplicate: true, adId: 3}]},
      {type: 'postbytespreview', listenerEpisode: 'listen4', timestamp: 1490837132, impressions: [{adId: 4}]},
      {type: 'postbytes', listenerEpisode: 'listen5', timestamp: 1490827132999, impressions: [{adId: 5}]}
    ]);
    return impression2.insert().then(result => {
      expect(result.length).to.equal(2);

      expect(result[0].dest).to.equal('dt_impressions');
      expect(result[0].count).to.equal(4);
      expect(inserts['dt_impressions'][0].json.listener_session.length).to.be.above(10);
      expect(inserts['dt_impressions'][0].json.ad_id).to.equal(1);
      expect(inserts['dt_impressions'][0].json.is_bytes).to.equal(false);
      expect(inserts['dt_impressions'][1].json.listener_session.length).to.be.above(10);
      expect(inserts['dt_impressions'][1].json.ad_id).to.equal(2);
      expect(inserts['dt_impressions'][1].json.is_bytes).to.equal(false);
      expect(inserts['dt_impressions'][2].json.listener_session.length).to.be.above(10);
      expect(inserts['dt_impressions'][2].json.ad_id).to.equal(3);
      expect(inserts['dt_impressions'][2].json.is_bytes).to.equal(false);
      expect(inserts['dt_impressions'][3].json.listener_session.length).to.be.above(10);
      expect(inserts['dt_impressions'][3].json.ad_id).to.equal(5);
      expect(inserts['dt_impressions'][3].json.is_bytes).to.equal(true);

      expect(result[1].dest).to.equal('dt_impressions_preview');
      expect(result[1].count).to.equal(1);
      expect(inserts['dt_impressions_preview'][0].json.listener_session.length).to.be.above(10);
      expect(inserts['dt_impressions_preview'][0].json.ad_id).to.equal(4);
      expect(inserts['dt_impressions_preview'][0].json.is_bytes).to.equal(true);
    });
  });

});
