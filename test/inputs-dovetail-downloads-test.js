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
    expect(download.check({ type: 'postbytes' })).to.be.false;
    expect(download.check({ type: 'postbytes', download: {} })).to.be.true;
  });

  it('knows the table names of records', () => {
    expect(download.tableName({ type: 'postbytes' })).to.equal('dt_downloads');
  });

  it('formats table inserts', async () => {
    const record = await download.format({
      type: 'postbytes',
      timestamp: 1490827132999,
      download: { isDuplicate: true, cause: 'whatever' },
      listenerEpisode: 'something',
      remoteIp: '1.2.3.4, 5.6.7.8',
      city: 888,
      country: 999,
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
      'postal_code',
      'zones_filled_pre',
      'zones_filled_mid',
      'zones_filled_post',
      'zones_filled_house_pre',
      'zones_filled_house_mid',
      'zones_filled_house_post',
      'zones_unfilled_pre',
      'zones_unfilled_mid',
      'zones_unfilled_post',
      'zones_unfilled_house_pre',
      'zones_unfilled_house_mid',
      'zones_unfilled_house_post',
    );
    expect(record.json.timestamp).to.equal(1490827132);
    expect(record.json.listener_episode).to.equal('something');
    expect(record.json.is_duplicate).to.equal(true);
    expect(record.json.cause).to.equal('whatever');
    expect(record.json.remote_ip).to.equal('1.2.3.0');
    expect(record.json.city_geoname_id).to.equal(888);
    expect(record.json.country_geoname_id).to.equal(999);
  });

  it('inserts fill rates', async () => {
    const record = await download.format({
      download: {},
      filled: {
        paid: [0, 1, 2],
        house: [3, 4, 5],
      },
      unfilled: {
        paid: [6, 7, 8],
        house: [9, 10, 11],
      },
    });

    expect(record.json.zones_filled_pre).to.equal(0);
    expect(record.json.zones_filled_mid).to.equal(1);
    expect(record.json.zones_filled_post).to.equal(2);
    expect(record.json.zones_filled_house_pre).to.equal(3);
    expect(record.json.zones_filled_house_mid).to.equal(4);
    expect(record.json.zones_filled_house_post).to.equal(5);
    expect(record.json.zones_unfilled_pre).to.equal(6);
    expect(record.json.zones_unfilled_mid).to.equal(7);
    expect(record.json.zones_unfilled_post).to.equal(8);
    expect(record.json.zones_unfilled_house_pre).to.equal(9);
    expect(record.json.zones_unfilled_house_mid).to.equal(10);
    expect(record.json.zones_unfilled_house_post).to.equal(11);
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
      { type: 'postbytes', download: {}, listenerEpisode: 'list-ep-1', timestamp: 1490827132999 },
      { type: 'impression', requestUuid: 'the-uuid2', timestamp: 1490827132999 },
      { type: 'postbytes', download: {}, listenerEpisode: 'list-ep-3', timestamp: 1490827132999 },
      { type: 'postbytes', download: {}, listenerEpisode: 'list-ep-4', timestamp: 1490827132999 },
      { type: 'postbytes', download: {}, listenerEpisode: 'list-ep-6', timestamp: 1490827132999 },
    ]);
    return download2.insert().then(result => {
      expect(result.length).to.equal(1);

      expect(result[0].dest).to.equal('dt_downloads');
      expect(result[0].count).to.equal(4);
      expect(inserts['dt_downloads'].length).to.equal(4);
      expect(inserts['dt_downloads'][0].json.listener_episode).to.equal('list-ep-1');
      expect(inserts['dt_downloads'][1].json.listener_episode).to.equal('list-ep-3');
      expect(inserts['dt_downloads'][2].json.listener_episode).to.equal('list-ep-4');
      expect(inserts['dt_downloads'][3].json.listener_episode).to.equal('list-ep-6');
    });
  });
});
