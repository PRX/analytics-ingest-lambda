'use strict';

const support = require('./support');
const bigquery = require('../lib/bigquery');
const DovetailImpressions = require('../lib/inputs/dovetail-impressions');

describe('dovetail-impressions', () => {

  let impression = new DovetailImpressions();

  it('recognizes impression records', () => {
    expect(impression.check({type: null})).to.be.false;
    expect(impression.check({type: undefined})).to.be.false;
    expect(impression.check({type: undefined, adId: 1})).to.be.true;
    expect(impression.check({type: 'download'})).to.be.false;
    expect(impression.check({type: 'impression'})).to.be.true;
  });

  it('partitions the table', () => {
    expect(impression.table({timestamp: 0})).to.equal('the_impressions_table$19700101');
    expect(impression.table({timestamp: 1490827132000})).to.equal('the_impressions_table$20170329');
    expect(impression.table({timestamp: 1490827132})).to.equal('the_impressions_table$20170329');
    expect(impression.table({timestamp: 1490837132})).to.equal('the_impressions_table$20170330');
  });

  it('formats table inserts', () => {
    return impression.format({requestUuid: 'the-uuid', timestamp: 1490827132999}).then(row => {
      expect(row).to.have.keys('table', 'record');
      expect(row.table).to.equal('the_impressions_table$20170329');
      expect(row.record).to.have.keys('insertId', 'json');
      expect(row.record.insertId).not.to.equal('the-uuid');
      expect(row.record.json).to.have.keys(
        'protocol',
        'host',
        'query',
        'digest',
        'program',
        'path',
        'feeder_podcast',
        'feeder_episode',
        'remote_agent',
        'remote_ip',
        'timestamp',
        'request_uuid',
        'ad_id',
        'campaign_id',
        'creative_id',
        'flight_id',
        'is_duplicate',
        'cause',
        'city_id',
        'country_id',
        'agent_name_id',
        'agent_type_id',
        'agent_os_id'
      );
      expect(row.record.json.timestamp).to.equal(1490827132);
      expect(row.record.json.request_uuid).to.equal('the-uuid');
    });
  });

  it('creates unique insert ids for ads', () => {
    let formats = [
      impression.format({requestUuid: 'req1', adId: 1}),
      impression.format({requestUuid: 'req1', adId: 1}),
      impression.format({requestUuid: 'req1', adId: 2}),
      impression.format({requestUuid: 'req2', adId: 1})
    ];
    return Promise.all(formats).then(datas => {
      expect(datas[0].record.insertId).to.equal(datas[1].record.insertId);
      expect(datas[0].record.insertId).not.to.equal(datas[2].record.insertId);
      expect(datas[0].record.insertId).not.to.equal(datas[3].record.insertId);
    });
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
    ]);
    return impression2.insert().then(result => {
      expect(result.length).to.equal(2);
      expect(result[0].dest).to.equal('the_impressions_table$20170329');
      expect(result[0].count).to.equal(2);
      expect(result[1].dest).to.equal('the_impressions_table$20170330');
      expect(result[1].count).to.equal(1);
      expect(inserts['the_impressions_table$20170329'].length).to.equal(2);
      expect(inserts['the_impressions_table$20170329'][0].json.request_uuid).to.equal('the-uuid1');
      expect(inserts['the_impressions_table$20170329'][1].json.request_uuid).to.equal('the-uuid4');
      expect(inserts['the_impressions_table$20170330'].length).to.equal(1);
      expect(inserts['the_impressions_table$20170330'][0].json.request_uuid).to.equal('the-uuid3');
    });
  });

});
