'use strict';

const support    = require('./support');
const impression = require('../lib/inputs/dovetail-impression');

describe('dovetail-impression', () => {

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
      expect(row).to.have.keys('insertId', 'json');
      expect(row.insertId).not.to.equal('the-uuid');
      expect(row.json).to.have.keys(
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
        'country_id'
      );
      expect(row.json.timestamp).to.equal(1490827132);
      expect(row.json.request_uuid).to.equal('the-uuid');
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
      expect(datas[0].insertId).to.equal(datas[1].insertId);
      expect(datas[0].insertId).not.to.equal(datas[2].insertId);
      expect(datas[0].insertId).not.to.equal(datas[3].insertId);
    });
  });

});
