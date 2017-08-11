'use strict';

const support  = require('./support');
const download = require('../lib/inputs/dovetail-download');

describe('dovetail-download', () => {

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
      expect(row).to.have.keys('insertId', 'json');
      expect(row.insertId).to.equal('the-uuid');
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
        'ad_count',
        'is_duplicate',
        'cause',
        'city_id',
        'country_id',
        'agent_name_id',
        'agent_type_id',
        'agent_os_id'
      );
      expect(row.json.timestamp).to.equal(1490827132);
      expect(row.json.request_uuid).to.equal('the-uuid');
    });
  });

});
