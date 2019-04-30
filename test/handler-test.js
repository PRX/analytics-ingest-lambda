'use strict';

const support   = require('./support');
const testRecs  = require('./support/test-records');
const bigquery  = require('../lib/bigquery');
const dynamo    = require('../lib/dynamo');
const kinesis   = require('../lib/kinesis');
const logger    = require('../lib/logger');
const index     = require('../index');

function handler(event) {
  return new Promise((resolve, reject) => {
    index.handler(event, {}, (err, res) => {
      err ? reject(err) : resolve(res);
    });
  });
}

describe('handler', () => {

  let event, infos, warns, errs;
  beforeEach(() => {
    event = support.buildEvent(testRecs);
    infos = [];
    warns = [];
    errs = [];
    sinon.stub(logger, 'info').callsFake(msg => infos.push(msg));
    sinon.stub(logger, 'error').callsFake(msg => errs.push(msg));
    sinon.stub(logger, 'warn').callsFake(msg => warns.push(msg));
  });

  it('complains about insane inputs', async () => {
    const result = await handler({foo: 'bar'});
    expect(result).to.be.undefined;
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/invalid event input/i);
  });

  it('complains about non-kinesis inputs', async () => {
    const result = await handler({Records: [{}]});
    expect(result).to.be.undefined;
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/invalid record input/i);
  });

  it('complains about json parse errors', async () => {
    const result = await handler({Records: [{kinesis: {data: 'not-json'}}]});
    expect(result).to.be.undefined;
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/invalid record input/i);
  });

  it('complains about unrecognized inputs', async () => {
    const result = await handler(support.buildEvent([{foo: 'bar'}]));
    expect(result).to.match(/inserted 0/i);
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/unrecognized input record/i);
  });

  it('handles bigquery records', async () => {
    const inserted = {};
    sinon.stub(bigquery, 'insert').callsFake((ds, tbl, rows) => {
      inserted[tbl] = rows;
      return Promise.resolve(rows.length);
    });

    const result = await handler(event);
    expect(result).to.match(/inserted 5/i);
    expect(infos.length).to.equal(3);
    expect(warns.length).to.equal(0);
    expect(errs.length).to.equal(0);
    expect(infos[0]).to.match(/1 rows into dt_downloads/);
    expect(infos[1]).to.match(/1 rows into dt_downloads_preview/);
    expect(infos[2]).to.match(/3 rows into dt_impressions/);

    // based on test-records
    expect(inserted).to.have.keys('dt_downloads', 'dt_downloads_preview', 'dt_impressions');

    expect(inserted['dt_downloads'].length).to.equal(1);
    expect(inserted['dt_downloads'][0].insertId).to.match(/^\w+\/1487703699$/);
    let downloadJson = inserted['dt_downloads'][0].json;
    expect(downloadJson.timestamp).to.equal(1487703699);
    expect(downloadJson.request_uuid).to.equal('req-uuid');
    expect(downloadJson.feeder_podcast).to.equal(1234);
    expect(downloadJson.feeder_episode).to.equal('1234-5678');
    expect(downloadJson.listener_id).to.equal('some-listener-id');
    expect(downloadJson.listener_episode).to.equal('listener-episode-1');
    expect(downloadJson.listener_session.length).to.be.above(10);
    expect(downloadJson.is_confirmed).to.equal(false);
    expect(downloadJson.is_bytes).to.equal(false);
    expect(downloadJson.digest).to.equal('the-digest');
    expect(downloadJson.ad_count).to.equal(2);
    expect(downloadJson.is_duplicate).to.equal(false);
    expect(downloadJson.cause).to.be.null;
    expect(downloadJson.remote_referrer).to.equal('https://www.prx.org/technology/');
    expect(downloadJson.remote_agent).to.equal('AppleCoreMedia/1.0.0.14B100 (iPhone; U; CPU OS 10_1_1 like Mac OS X; en_us)');
    expect(downloadJson.remote_ip).to.equal('24.49.134.0');
    expect(downloadJson.agent_name_id).to.equal(25);
    expect(downloadJson.agent_type_id).to.equal(36);
    expect(downloadJson.agent_os_id).to.equal(43);
    expect(downloadJson.city_geoname_id).to.equal(5576882);
    expect(downloadJson.country_geoname_id).to.equal(6252001);
    expect(downloadJson.postal_code).to.equal('80517');
    expect(Math.abs(downloadJson.latitude - 40.3772)).to.be.below(1);
    expect(Math.abs(downloadJson.longitude + 105.5217)).to.be.below(1);

    expect(inserted['dt_impressions'].length).to.equal(3);
    expect(inserted['dt_impressions'][0].insertId.length).to.be.above(10);
    expect(inserted['dt_impressions'][1].insertId.length).to.be.above(10);
    expect(inserted['dt_impressions'][2].insertId.length).to.be.above(10);
    expect(inserted['dt_impressions'][0].insertId).not.to.equal(inserted['dt_impressions'][1].insertId);
    expect(inserted['dt_impressions'][0].insertId).not.to.equal(inserted['dt_impressions'][2].insertId);
    expect(inserted['dt_impressions'][1].insertId).not.to.equal(inserted['dt_impressions'][2].insertId);

    let impressionJson = inserted['dt_impressions'][0].json;
    expect(impressionJson.timestamp).to.equal(1487703699);
    expect(impressionJson.request_uuid).to.equal('req-uuid');
    expect(impressionJson.feeder_podcast).to.equal(1234);
    expect(impressionJson.feeder_episode).to.equal('1234-5678');
    expect(impressionJson.listener_session).to.equal(downloadJson.listener_session);
    expect(impressionJson.digest).to.equal('the-digest');
    expect(impressionJson.segment).to.equal(0);
    expect(impressionJson.is_confirmed).to.equal(false);
    expect(impressionJson.is_bytes).to.equal(false);
    expect(impressionJson.is_duplicate).to.equal(false);
    expect(impressionJson.cause).to.be.null;
    expect(impressionJson.ad_id).to.equal(12);
    expect(impressionJson.campaign_id).to.equal(34);
    expect(impressionJson.creative_id).to.equal(56);
    expect(impressionJson.flight_id).to.equal(78);

    impressionJson = inserted['dt_impressions'][1].json;
    expect(impressionJson.ad_id).to.equal(98);
    expect(impressionJson.segment).to.equal(0);
    expect(impressionJson.is_confirmed).to.equal(true);
    expect(impressionJson.is_duplicate).to.equal(true);
    expect(impressionJson.cause).to.equal('something');

    impressionJson = inserted['dt_impressions'][2].json;
    expect(impressionJson.ad_id).to.equal(76);
    expect(impressionJson.segment).to.equal(3);
    expect(impressionJson.is_confirmed).to.equal(true);
    expect(impressionJson.is_duplicate).to.equal(false);
    expect(impressionJson.cause).to.equal(null);

    let previewJson = inserted['dt_downloads_preview'][0].json;
    expect(previewJson.listener_session.length).to.be.above(10);
    expect(previewJson.listener_session).not.to.equal(downloadJson.listener_session);
    expect(previewJson.feeder_podcast).to.equal(1234);
    expect(previewJson.feeder_episode).to.equal('1234-5678');
    expect(previewJson.is_bytes).to.equal(true);
    expect(previewJson.is_duplicate).to.equal(false);
    expect(previewJson.cause).to.equal(null);
  });

  it('handles dynamodb records', async () => {
    sinon.stub(dynamo, 'write').callsFake(async (recs) => recs.length);
    sinon.stub(dynamo, 'get').callsFake(async () => [
      {id: 'listener-episode-3.the-digest', type: 'antebytes', any: 'thing', download: {}, impressions: [
        {segment: 0, pings: ['ping', 'backs']},
        {segment: 1, pings: ['ping', 'backs']},
        {segment: 2, pings: ['ping', 'backs']},
      ]},
    ]);
    sinon.stub(kinesis, 'put').callsFake(async (datas) => datas.length);
    process.env.DYNAMODB = 'true';

    const result = await handler(event);
    expect(result).to.match(/inserted 3/i);
    expect(infos.length).to.equal(2);
    expect(warns.length).to.equal(1);
    expect(errs.length).to.equal(0);
    expect(infos[0]).to.match(/inserted 2 rows into dynamodb/i);
    expect(infos[1]).to.match(/inserted 1 rows into kinesis/i);
    expect(warns[0]).to.match(/missing segment listener-episode-3.the-digest.4/i);

    expect(dynamo.write.args[0][0].length).to.equal(2);
    expect(dynamo.write.args[0][0][0].type).to.equal('antebytes');
    expect(dynamo.write.args[0][0][0].any).to.equal('thing');
    expect(dynamo.write.args[0][0][0].id).to.equal('listener-episode-4.the-digest');
    expect(dynamo.write.args[0][0][0].listenerEpisode).to.be.undefined;
    expect(dynamo.write.args[0][0][0].digest).to.be.undefined;
    expect(dynamo.write.args[0][0][1].type).to.equal('antebytespreview');
    expect(dynamo.write.args[0][0][1].some).to.equal('thing');
    expect(dynamo.write.args[0][0][1].id).to.equal('listener-episode-5.the-digest');
    expect(dynamo.write.args[0][0][1].listenerEpisode).to.be.undefined;
    expect(dynamo.write.args[0][0][1].digest).to.be.undefined;

    expect(kinesis.put.args[0][0].length).to.equal(1);
    expect(kinesis.put.args[0][0][0].type).to.equal('postbytes');
    expect(kinesis.put.args[0][0][0].listenerEpisode).to.equal('listener-episode-3');
    expect(kinesis.put.args[0][0][0].digest).to.equal('the-digest');
    expect(kinesis.put.args[0][0][0].any).to.equal('thing');
  });

  it('handles pingback records', async () => {
    process.env.PINGBACKS = 'true';
    const ping1 = nock('http://www.foo.bar').get('/ping1').reply(200);
    const ping2 = nock('http://www.foo.bar').get('/ping2').reply(404);
    const ping3 = nock('http://www.foo.bar').get('/ping3').reply(200);
    const ping4 = nock('http://www.foo.bar').get('/ping4').reply(200);

    const result = await handler(event);
    expect(result).to.match(/inserted 2/i);
    expect(infos.length).to.equal(1);
    expect(warns.length).to.equal(1);
    expect(errs.length).to.equal(0);
    expect(infos[0]).to.match(/2 rows into www.foo.bar/);
    expect(warns[0]).to.match(/PINGFAIL error: http 404/i);
    expect(warns[0]).to.match(/ping2/);

    expect(ping1.isDone()).to.be.true;
    expect(ping2.isDone()).to.be.true;
    expect(ping3.isDone()).to.be.false;
    expect(ping4.isDone()).to.be.true;
  });

  it('handles redis records', async () => {
    process.env.REDIS_HOST = 'redis://127.0.0.1:6379';
    const result = await handler(event);
    expect(result).to.match(/inserted 4/i);
    expect(infos.length).to.equal(1);
    expect(warns.length).to.equal(0);
    expect(errs.length).to.equal(0);
    expect(infos[0]).to.match(/4 rows into redis:/);

    let keys = [
      support.redisKeys('downloads.episodes.*'),
      support.redisKeys('downloads.podcasts.*'),
      support.redisKeys('impressions.episodes.*'),
      support.redisKeys('impressions.podcasts.*')
    ];
    const all = await Promise.all(keys);
    expect(all[0].length).to.equal(2);
    expect(all[1].length).to.equal(2);
    expect(all[2].length).to.equal(0);
    expect(all[3].length).to.equal(0);
  });

});
