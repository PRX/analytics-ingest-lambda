'use strict';

const support = require('./support');
const bigquery = require('../lib/bigquery');
const LegacyDovetailDownloads = require('../lib/inputs/legacy-dovetail-downloads');

describe('legacy-dovetail-downloads', () => {

  let download = new LegacyDovetailDownloads();

  it('recognizes download records', () => {
    expect(download.check({})).to.be.false;
    expect(download.check({type: 'impression'})).to.be.false;
    expect(download.check({type: 'download'})).to.be.true;
  });

  it('formats table inserts', () => {
    return download.format({requestUuid: 'the-uuid', timestamp: 1490827132999}).then(row => {
      expect(row).to.have.keys('table', 'record');
      expect(row.table).to.equal('dt_downloads');
      expect(row.record).to.have.keys('insertId', 'json');
      expect(row.record.insertId).to.equal('the-uuid');
      expect(row.record.json).to.have.keys(
        'timestamp',
        'request_uuid',
        'feeder_podcast',
        'feeder_episode',
        'program',
        'path',
        'clienthash',
        'digest',
        'ad_count',
        'is_duplicate',
        'cause',
        'remote_referrer',
        'remote_agent',
        'remote_ip',
        'agent_name_id',
        'agent_type_id',
        'agent_os_id',
        'city_geoname_id',
        'country_geoname_id',
        'postal_code',
        'latitude',
        'longitude'
      );
      expect(row.record.json.timestamp).to.equal(1490827132);
      expect(row.record.json.request_uuid).to.equal('the-uuid');
    });
  });

  it('inserts nothing', () => {
    return download.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts download records', () => {
    let inserts = {};
    sinon.stub(bigquery, 'insert').callsFake((tbl, rows) => {
      inserts[tbl] = rows;
      return Promise.resolve(rows.length);
    });
    let download2 = new LegacyDovetailDownloads([
      {type: 'download', requestUuid: 'the-uuid1', timestamp: 1490827132999},
      {type: 'impression', requestUuid: 'the-uuid2', timestamp: 1490827132999},
      {type: 'download', requestUuid: 'the-uuid3', timestamp: 1490837132},
      {type: 'download', requestUuid: 'the-uuid4', timestamp: 1490827132999},
    ]);
    return download2.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('dt_downloads');
      expect(result[0].count).to.equal(3);
      expect(inserts['dt_downloads'].length).to.equal(3);
      expect(inserts['dt_downloads'][0].json.request_uuid).to.equal('the-uuid1');
      expect(inserts['dt_downloads'][1].json.request_uuid).to.equal('the-uuid3');
      expect(inserts['dt_downloads'][2].json.request_uuid).to.equal('the-uuid4');
    });
  });

});
