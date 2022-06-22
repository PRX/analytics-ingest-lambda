'use strict';

const support = require('./support');
const bigquery = require('../lib/bigquery');
const DovetailDownloads = require('../lib/inputs/dovetail-downloads');

describe('dovetail-downloads', () => {
  let download = new DovetailDownloads();

  it('recognizes download records', () => {
    expect(download.check({})).to.be.false;
    expect(download.check({ type: 'impression' })).to.be.false;
    expect(download.check({ type: 'download' })).to.be.false;
    expect(download.check({ type: 'combined', download: null })).to.be.false;
    expect(download.check({ type: 'combined', download: {} })).to.be.true;
    expect(download.check({ type: 'postbytes' })).to.be.false;
    expect(download.check({ type: 'postbytes', download: {} })).to.be.true;
    expect(download.check({ type: 'postbytespreview' })).to.be.false;
    expect(download.check({ type: 'postbytespreview', download: {} })).to.be.true;
  });

  it('knows the table names of records', () => {
    expect(download.tableName({ type: 'combined' })).to.equal('dt_downloads');
    expect(download.tableName({ type: 'postbytes' })).to.equal('dt_downloads');
    expect(download.tableName({ type: 'postbytespreview' })).to.equal('dt_downloads_preview');
  });

  it('formats table inserts', async () => {
    const record = await download.format({
      type: 'combined',
      timestamp: 1490827132999,
      download: { isDuplicate: true, cause: 'whatever' },
      listenerEpisode: 'something',
    });
    expect(record).to.have.keys('insertId', 'json');
    expect(record.insertId).to.match(/^\w+\/1490827132$/);
    expect(record.json).to.have.keys(
      'timestamp',
      'request_uuid',
      'feeder_podcast',
      'feeder_feed',
      'feeder_episode',
      'digest',
      'ad_count',
      'is_duplicate',
      'cause',
      'is_confirmed',
      'url',
      'listener_id',
      'listener_episode',
      'remote_referrer',
      'remote_agent',
      'remote_ip',
      'agent_name_id',
      'agent_type_id',
      'agent_os_id',
      'city_geoname_id',
      'country_geoname_id',
    );
    expect(record.json.timestamp).to.equal(1490827132);
    expect(record.json.listener_episode).to.equal('something');
    expect(record.json.is_duplicate).to.equal(true);
    expect(record.json.cause).to.equal('whatever');
  });

  it('inserts nothing', () => {
    return download.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts download records', () => {
    let inserts = {};
    sinon.stub(bigquery, 'insert').callsFake((ds, tbl, rows) => {
      inserts[tbl] = rows;
      return Promise.resolve(rows.length);
    });
    let download2 = new DovetailDownloads([
      { type: 'download', requestUuid: 'the-uuid0', timestamp: 1490827132999 },
      { type: 'combined', download: {}, listenerEpisode: 'list-ep-1', timestamp: 1490827132999 },
      { type: 'impression', requestUuid: 'the-uuid2', timestamp: 1490827132999 },
      { type: 'combined', download: {}, listenerEpisode: 'list-ep-3', timestamp: 1490827132999 },
      { type: 'combined', download: {}, listenerEpisode: 'list-ep-4', timestamp: 1490827132999 },
      {
        type: 'postbytespreview',
        download: {},
        listenerEpisode: 'list-ep-5',
        timestamp: 1490827132999,
      },
      { type: 'postbytes', download: {}, listenerEpisode: 'list-ep-6', timestamp: 1490827132999 },
    ]);
    return download2.insert().then(result => {
      expect(result.length).to.equal(2);

      expect(result[0].dest).to.equal('dt_downloads');
      expect(result[0].count).to.equal(4);
      expect(inserts['dt_downloads'].length).to.equal(4);
      expect(inserts['dt_downloads'][0].json.listener_episode).to.equal('list-ep-1');
      expect(inserts['dt_downloads'][1].json.listener_episode).to.equal('list-ep-3');
      expect(inserts['dt_downloads'][2].json.listener_episode).to.equal('list-ep-4');
      expect(inserts['dt_downloads'][3].json.listener_episode).to.equal('list-ep-6');

      expect(result[1].dest).to.equal('dt_downloads_preview');
      expect(result[1].count).to.equal(1);
      expect(inserts['dt_downloads_preview'].length).to.equal(1);
      expect(inserts['dt_downloads_preview'][0].json.listener_episode).to.equal('list-ep-5');
    });
  });
});
