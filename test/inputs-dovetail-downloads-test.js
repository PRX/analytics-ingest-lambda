'use strict';

const support = require('./support');
const bigquery = require('../lib/bigquery');
const DovetailDownloads = require('../lib/inputs/dovetail-downloads');

describe('dovetail-downloads', () => {

  let download = new DovetailDownloads();

  it('recognizes download records', () => {
    expect(download.check({})).to.be.false;
    expect(download.check({type: 'impression'})).to.be.false;
    expect(download.check({type: 'download'})).to.be.true;
  });

  it('partitions the table', () => {
    expect(download.table({timestamp: 0})).to.equal('the_downloads_table$19700101');
    expect(download.table({timestamp: 1490827132000})).to.equal('the_downloads_table$20170329');
    expect(download.table({timestamp: 1490827132})).to.equal('the_downloads_table$20170329');
    expect(download.table({timestamp: 1490837132})).to.equal('the_downloads_table$20170330');
  });

  it('formats table inserts', () => {
    return download.format({requestUuid: 'the-uuid', timestamp: 1490827132999}).then(row => {
      expect(row).to.have.keys('table', 'record');
      expect(row.table).to.equal('the_downloads_table$20170329');
      expect(row.record).to.have.keys('insertId', 'json');
      expect(row.record.insertId).to.equal('the-uuid');
      expect(row.record.json).to.have.keys(
        'digest',
        'program',
        'path',
        'feeder_podcast',
        'feeder_episode',
        'remote_agent',
        'remote_ip',
        'timestamp',
        'request_uuid',
        'ad_count',
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

  it('inserts nothing', () => {
    return download.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts download records', () => {
    let inserts = {};
    sinon.stub(bigquery, 'insert', (tbl, rows) => {
      inserts[tbl] = rows;
      return Promise.resolve(rows.length);
    });
    let download2 = new DovetailDownloads([
      {type: 'download', requestUuid: 'the-uuid1', timestamp: 1490827132999},
      {type: 'impression', requestUuid: 'the-uuid2', timestamp: 1490827132999},
      {type: 'download', requestUuid: 'the-uuid3', timestamp: 1490837132},
      {type: 'download', requestUuid: 'the-uuid4', timestamp: 1490827132999},
    ]);
    return download2.insert().then(result => {
      expect(result.length).to.equal(2);
      expect(result[0].dest).to.equal('the_downloads_table$20170329');
      expect(result[0].count).to.equal(2);
      expect(result[1].dest).to.equal('the_downloads_table$20170330');
      expect(result[1].count).to.equal(1);
      expect(inserts['the_downloads_table$20170329'].length).to.equal(2);
      expect(inserts['the_downloads_table$20170329'][0].json.request_uuid).to.equal('the-uuid1');
      expect(inserts['the_downloads_table$20170329'][1].json.request_uuid).to.equal('the-uuid4');
      expect(inserts['the_downloads_table$20170330'].length).to.equal(1);
      expect(inserts['the_downloads_table$20170330'][0].json.request_uuid).to.equal('the-uuid3');
    });
  });

});
