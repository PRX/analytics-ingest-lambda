'use strict';

const support = require('./support');
const testRecs = require('./support/test-records');
const bigquery = require('../lib/bigquery');
const dynamo = require('../lib/dynamo');
const logger = require('../lib/logger');
const index = require('../index');

async function handler(event) {
  return await index.handler(event);
}

describe('handler', () => {
  let event, infos, warns, errs;
  beforeEach(() => {
    event = support.buildEvent(testRecs);
    infos = [];
    warns = [];
    errs = [];
    sinon.stub(logger, 'info').callsFake((msg, meta) => infos.push({ msg, meta }));
    sinon.stub(logger, 'error').callsFake(msg => errs.push(msg));
    sinon.stub(logger, 'warn').callsFake(msg => warns.push(msg));
  });
  afterEach(() => {
    process.env.PROCESS_AFTER = '';
    process.env.PROCESS_UNTIL = '';
  });

  it('complains about insane inputs', async () => {
    const result = await handler({ foo: 'bar' });
    expect(result).to.be.undefined;
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/invalid event input/i);
  });

  it('complains about non-kinesis inputs', async () => {
    const result = await handler({ Records: [{}] });
    expect(result).to.be.undefined;
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/invalid record input/i);
  });

  it('complains about json parse errors', async () => {
    const result = await handler({ Records: [{ kinesis: { data: 'not-json' } }] });
    expect(result).to.be.undefined;
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/invalid record input/i);
  });

  it('complains about unrecognized inputs', async () => {
    const result = await handler(support.buildEvent([{ foo: 'bar' }]));
    expect(result).to.match(/inserted 0/i);
    expect(errs.length).to.equal(1);
    expect(errs[0]).to.match(/unrecognized input record/i);
  });

  it('filters out records before a timestamp', async () => {
    process.env.PROCESS_AFTER = 1000;
    const result = await handler(support.buildEvent([{ timestamp: 900 }, { timestamp: 1000 }]));
    expect(result).to.be.undefined;
  });

  it('filters out records after a timestamp', async () => {
    process.env.PROCESS_UNTIL = 1000;
    const result = await handler(support.buildEvent([{ timestamp: 1001 }, { timestamp: 1100 }]));
    expect(result).to.be.undefined;
  });

  it('handles bigquery records', async () => {
    const inserted = {};
    sinon.stub(bigquery, 'insert').callsFake((ds, tbl, rows) => {
      inserted[tbl] = rows.sort((a, b) => (a.insertId < b.insertId ? -1 : 1));
      return Promise.resolve(rows.length);
    });

    const result = await handler(event);
    expect(result).to.match(/inserted 7/i);
    expect(infos.length).to.equal(5);
    expect(warns.length).to.equal(0);
    expect(errs.length).to.equal(0);
    expect(infos[0].msg).to.equal('Event records');
    expect(infos[0].meta).to.eql({ raw: 10, decoded: 10 });
    expect(infos[1].msg).to.match(/1 rows into dt_downloads/);
    expect(infos[1].meta).to.contain({ dest: 'dt_downloads', rows: 1 });
    expect(infos[2].msg).to.match(/1 rows into dt_downloads_preview/);
    expect(infos[2].meta).to.contain({ dest: 'dt_downloads_preview', rows: 1 });
    expect(infos[3].msg).to.match(/4 rows into dt_impressions/);
    expect(infos[3].meta).to.contain({ dest: 'dt_impressions', rows: 4 });

    // based on test-records
    expect(inserted).to.have.keys(
      'dt_downloads',
      'dt_downloads_preview',
      'dt_impressions',
      'pixels',
    );

    expect(inserted['dt_downloads'].length).to.equal(1);
    expect(inserted['dt_downloads'][0].insertId).to.equal('listener-episode-1/1487703699');
    let downloadJson = inserted['dt_downloads'][0].json;
    expect(downloadJson.timestamp).to.equal(1487703699);
    expect(downloadJson.request_uuid).to.equal('req-uuid');
    expect(downloadJson.feeder_podcast).to.equal(1234);
    expect(downloadJson.feeder_episode).to.equal('1234-5678');
    expect(downloadJson.listener_id).to.equal('some-listener-id');
    expect(downloadJson.listener_episode).to.equal('listener-episode-1');
    expect(downloadJson.is_confirmed).to.equal(false);
    expect(downloadJson.digest).to.equal('the-digest');
    expect(downloadJson.ad_count).to.equal(2);
    expect(downloadJson.is_duplicate).to.equal(false);
    expect(downloadJson.cause).to.be.null;
    expect(downloadJson.remote_referrer).to.equal('https://www.prx.org/technology/');
    expect(downloadJson.remote_agent).to.equal(
      'AppleCoreMedia/1.0.0.14B100 (iPhone; U; CPU OS 10_1_1 like Mac OS X; en_us)',
    );
    expect(downloadJson.remote_ip).to.equal('24.49.134.0');
    expect(downloadJson.agent_name_id).to.equal(25);
    expect(downloadJson.agent_type_id).to.equal(36);
    expect(downloadJson.agent_os_id).to.equal(43);
    expect(downloadJson.city_geoname_id).to.equal(5576882);
    expect(downloadJson.country_geoname_id).to.equal(6252001);

    expect(inserted['dt_impressions'].length).to.equal(4);
    expect(inserted['dt_impressions'][0].insertId.length).to.be.above(10);
    expect(inserted['dt_impressions'][1].insertId.length).to.be.above(10);
    expect(inserted['dt_impressions'][2].insertId.length).to.be.above(10);
    expect(inserted['dt_impressions'][0].insertId).not.to.equal(
      inserted['dt_impressions'][1].insertId,
    );
    expect(inserted['dt_impressions'][0].insertId).not.to.equal(
      inserted['dt_impressions'][2].insertId,
    );
    expect(inserted['dt_impressions'][1].insertId).not.to.equal(
      inserted['dt_impressions'][2].insertId,
    );

    let impressionJson = inserted['dt_impressions'][0].json;
    expect(impressionJson.timestamp).to.equal(1487703699);
    expect(impressionJson.request_uuid).to.equal('req-uuid');
    expect(impressionJson.feeder_podcast).to.equal(1234);
    expect(impressionJson.feeder_episode).to.equal('1234-5678');
    expect(impressionJson.digest).to.equal('the-digest');
    expect(impressionJson.segment).to.equal(0);
    expect(impressionJson.is_confirmed).to.equal(false);
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

    impressionJson = inserted['dt_impressions'][3].json;
    expect(impressionJson.vast_advertiser).to.equal('vastadvertiser1');
    expect(impressionJson.vast_ad_id).to.equal('vastad1');
    expect(impressionJson.vast_creative_id).to.equal('vastcreative1');
    expect(impressionJson.vast_price_value).to.equal(10.0);
    expect(impressionJson.vast_price_currency).to.equal('USD');
    expect(impressionJson.vast_price_model).to.equal('CPM');

    let previewJson = inserted['dt_downloads_preview'][0].json;
    expect(previewJson.feeder_podcast).to.equal(1234);
    expect(previewJson.feeder_episode).to.equal('1234-5678');
    expect(previewJson.is_duplicate).to.equal(false);
    expect(previewJson.cause).to.equal(null);

    expect(inserted['pixels'].length).to.equal(1);
    expect(inserted['pixels'][0].json).to.eql({
      canonical: 'https://www.prx.org/url1',
      city_geoname_id: null,
      country_geoname_id: null,
      key: 'key1',
      remote_agent: 'some-user-agent',
      remote_ip: '127.0.0.0',
      remote_referrer: 'https://www.prx.org/technology/',
      timestamp: 1490827132,
      user_id: 'd24a63774631fde164fa2bc27e58db5e',
    });
  });

  it('handles dynamodb records', async () => {
    const payload = await dynamo.deflate({
      type: 'antebytes',
      any: 'thing',
      download: {},
      impressions: [
        { segment: 0, pings: ['ping', 'backs'] },
        { segment: 1, pings: ['ping', 'backs'] },
        { segment: 2, pings: ['ping', 'backs'] },
      ],
    });

    // return the redirect-data when particular updateItem is called
    sinon.stub(dynamo, 'updateItemPromise').callsFake(async params => {
      if (params.Key.id.S === 'listener-episode-3.the-digest') {
        return { Attributes: { payload: { B: payload } } };
      } else {
        return {};
      }
    });
    process.env.DYNAMODB = 'true';

    const result = await handler(event);
    expect(result).to.match(/inserted 5/i);
    expect(infos.length).to.equal(4);
    expect(warns.length).to.equal(0);
    expect(errs.length).to.equal(0);
    expect(infos[0].msg).to.equal('Event records');
    expect(infos[0].meta).to.eql({ raw: 10, decoded: 10 });
    expect(infos[1].msg).to.equal('impression');
    expect(infos[1].meta).to.contain({
      type: 'postbytes',
      listenerEpisode: 'listener-episode-3',
      digest: 'the-digest',
      any: 'thing',
    });
    expect(infos[2].msg).to.match(/inserted 4 rows into dynamodb/i);
    expect(infos[2].meta).to.contain({ dest: 'dynamodb', rows: 4 });
    expect(infos[3].msg).to.match(/inserted 1 rows into kinesis/i);
    expect(infos[3].meta).to.contain({ dest: 'kinesis', rows: 1 });

    const sortedArgs = dynamo.updateItemPromise.args.sort((a, b) => {
      return a[0].Key.id.S < b[0].Key.id.S ? -1 : 1;
    });

    const keys = sortedArgs.map(a => a[0].Key.id.S);
    expect(keys).to.eql([
      'listener-episode-3.the-digest',
      'listener-episode-4.the-digest',
      'listener-episode-5.the-digest',
      'listener-episode-dtrouter-1.the-digest',
    ]);

    const payloads = await Promise.all(
      sortedArgs.map(async a => {
        if (a[0].AttributeUpdates.payload) {
          return dynamo.inflate(a[0].AttributeUpdates.payload.Value.B);
        }
      }),
    );
    expect(payloads[0]).to.be.undefined;
    expect(payloads[1].type).to.equal('antebytes');
    expect(payloads[1].any).to.equal('thing');
    expect(payloads[2].type).to.equal('antebytespreview');
    expect(payloads[2].some).to.equal('thing');
    expect(payloads[3].type).to.equal('antebytes');
    expect(payloads[3].time).to.equal('2020-02-02T13:43:22.255Z');

    const segments = sortedArgs.map(a => {
      if (a[0].AttributeUpdates.segments) {
        return a[0].AttributeUpdates.segments.Value.SS;
      }
    });
    expect(segments[0]).to.eql(['1539287413617', '1539287527.4']);
    expect(segments[1]).to.be.undefined;
    expect(segments[2]).to.be.undefined;
    expect(segments[3]).to.be.undefined;
  });

  it('throws dynamodb throttling errors', async () => {
    const bad = new Error('Blah blah throughput exceeded');
    bad.name = 'ProvisionedThroughputExceededException';
    bad.statusCode = 400;
    sinon.stub(dynamo, 'updateItemPromise').rejects(bad);

    let err = null;
    try {
      process.env.DYNAMODB = 'true';
      const rec = { type: 'bytes', timestamp: 1234, listenerEpisode: 'le', digest: 'd' };
      await handler(support.buildEvent([rec]));
    } catch (e) {
      err = e;
    }
    if (err) {
      expect(err.message).to.match(/DDB retrying/);
      expect(warns.length).to.equal(2);
      expect(warns[0]).to.match(/throughput exceeded/);
      expect(warns[1]).to.match(/DDB retrying/);
      expect(errs.length).to.equal(0);
    } else {
      expect.fail('should have thrown an error');
    }
  });

  it('handles pingback records', async () => {
    process.env.PINGBACKS = 'true';
    const ping1 = nock('http://www.foo.bar').get('/ping1').reply(200);
    const ping2 = nock('http://www.foo.bar').get('/ping2').reply(404);
    const ping3 = nock('http://www.foo.bar').get('/ping3').reply(200);
    const ping4 = nock('http://www.adzerk.bar').get('/ping4').reply(200);

    const result = await handler(event);
    expect(result).to.match(/inserted 2/i);
    expect(infos.length).to.equal(4);
    expect(warns.length).to.equal(2);
    expect(errs.length).to.equal(0);
    expect(infos[0].msg).to.equal('Event records');
    expect(infos[0].meta).to.eql({ raw: 10, decoded: 10 });
    expect(infos[1].msg).to.equal('PINGED');
    expect(infos[1].meta).to.contain({ url: 'http://www.foo.bar/ping1' });
    expect(infos[2].msg).to.match(/1 rows into www.foo.bar/);
    expect(infos[2].meta).to.contain({ dest: 'www.foo.bar', rows: 1 });
    expect(infos[3].msg).to.match(/1 rows into www.adzerk.bar/);
    expect(infos[3].meta).to.contain({ dest: 'www.adzerk.bar', rows: 1 });
    expect(warns[0]).to.match(/PINGFAIL error: http 404/i);
    expect(warns[0]).to.match(/ping2/);

    expect(ping1.isDone()).to.be.true;
    expect(ping2.isDone()).to.be.true;
    expect(ping3.isDone()).to.be.false;
    expect(ping4.isDone()).to.be.true;
  });

  it('handles redis records', async () => {
    process.env.REDIS_HOST = 'redis://127.0.0.1:6379';
    process.env.REDIS_IMPRESSIONS_HOST = 'cluster://127.0.0.1:6379';
    const result = await handler(event);
    expect(result).to.match(/inserted 6/i);
    expect(infos.length).to.equal(3);
    expect(warns.length).to.equal(0);
    expect(errs.length).to.equal(0);
    expect(infos[0].msg).to.equal('Event records');
    expect(infos[0].meta).to.eql({ raw: 10, decoded: 10 });
    expect(infos[1].msg).to.match(/2 rows into cluster:/);
    expect(infos[1].meta).to.contain({ dest: 'cluster://127.0.0.1', rows: 2 });
    expect(infos[2].msg).to.match(/4 rows into redis:/);
    expect(infos[2].meta).to.contain({ dest: 'redis://127.0.0.1', rows: 4 });

    let keys = [
      support.redisKeys('castle:downloads.episodes.*'),
      support.redisKeys('castle:downloads.podcasts.*'),
      support.redisKeys('dovetail:impression:*'),
    ];
    const all = await Promise.all(keys);
    expect(all[0].length).to.equal(2);
    expect(all[1].length).to.equal(2);
    expect(all[2].length).to.equal(1);
  });

  it('handles unknown input records parsed from the log subscription filter style kinesis input', async () => {
    /* unknown records in the format of
    {
        "id": "35238420546692656231100808647663783122640070475706662913",
        "timestamp": 1580145427114,
        "message": "{\"msg\":\"impression\",\"testing\":\" newline\"}\n"
    } */

    const result = await handler({
      Records: [
        {
          kinesis: {
            data: 'H4sIAAg0L14AA9VSO0/DMBjc+ysqi7EoftvpFonQBRaSrUHIadwoUl5KXCpU9b/zOaUtMHVDeIit3Pl8d/ZhNoeBGjuOprTpR2/Rco4eojR6e46TJFrFaHGidPvWDh7UlAkOH0IoO4N1V66Gbtd7PDD7MahNkxcmqJp+AOmqa++BUlZt+W1H4gZrGr+FYooDTAKqgvXdU5TGSfq6lZoWmJGc6JBTlWvQUybkYlvkhhN8Fhp3+bgZqt7BIY9V7ewwguR6AifCS9e5aLMBG2j6+Xp1EL/b1v2kHy6riVQV3h8TlGlOseBScg2T4lhhpvxMmMaKEKklDrlSTAkqYQGJ9NnjRc1VULQzje+JCI0JFxANivzF+7oOf/QhQ81YZmiZoWuZGVpkyIEUFDpB89bu66q1GTpmLbqIHRe35wqpFNLfKgbjWnIlJVOaEUolx5CWK6GwlJKGhN2ai/+vXPzPc50e6Ow4+wT7VNt9mAMAAA==',
          },
        },
      ],
    });
    expect(result).to.match(/inserted 0/i);
    expect(errs.length).to.equal(3);
    expect(errs[0]).to.match(/Unrecognized input record/i);
    expect(errs[0]).to.match(/"msg":"impression"/i);
    expect(infos.length).to.equal(1);
    expect(infos[0].msg).to.equal('Event records');
    expect(infos[0].meta).to.eql({ raw: 1, decoded: 3 });
  });

  it('handles dt-router antebytes input records parsed from the log subscription filter style kinesis input', async () => {
    /* records in the format of
    {
        "id": "35238420546692656231100808647663783122640070475706662913",
        "timestamp": 1580145427114,
        "message": "{\"msg\":\"impression\",\"type\":\"antebytes\" ..... }\n"
    } */
    sinon.stub(dynamo, 'updateItemPromise').callsFake(async () => ({}));
    process.env.DYNAMODB = 'true';
    const result = await handler({
      Records: [
        {
          kinesis: {
            data: 'H4sIAAMrN14AA41UW3OiSBR+z6+gqH2YqQpCN80t++RGzTiOM050czVFNXDArgBNoNVxUvnv2zTG6L7sWpbgd75z+c453a9nmvzoBTQNzWCxq0C/0PRBf9EPp8P5vH811M87Ct+WULdGH9sOkT8IYfvdmPPsqubrqrWbdNuYOS2ihJqsqGoZmvHSkJSMldmRx1zUQIvWBVvYMi1kYs98/ONbfzGcL55S18eJZaMI+QHBXuTLeB4NiJMmESXIeg/UrKMmrlklZJIRywXUjQz5qIyKcM256MexLENX4NNHBcMNlOKU/np4UySWtPXZDrZ9gi2HuC7x5cMjlmfZXvtEtm95CLm+awXE82zPwa58kYr89xoP0QSTjRa0aPuEHN9CxJHSZCP/xduPo039ulReS/1iqdpkWFh+F8i+IPYFxj3sOA9L/Xyp57CBXNFYmXIFFU3WAYcpKDjmZcrqAhJpTGnegMQSlsnKFHtbNpuf2583sxHkd8XDfbh5odMfzctuNbDvy1E53d6Wi+f1HTHGKlwiNyPntI0mi6XJJV+XbaRynedtNrpu4OMvawbrKmcxFfCe/k3CKUAC9bBiDU86sXGMk8gnsUH91DUIIq5BcRIYCcQupuAQxyUqf+c640lMlQKM7PNjzY3EHlVl4+S4rKKiLCtPsZNSY7megm3gmJLmLFuJY+RU0B6schpD0S7XBHZKjth2M2kga/EPqqB1BmJGxeoD+81L+E73UxdyMGEnMpSaQrTU357agbNGQHnatOkXw7WJYL/ocLeKrtiGe/YkmTe3N7PJeLB1jfn96vn6u1WH025p9jGUIKkOBREOwDJs7GGDRMgxArndBgGgAbYxJAEovxoKLqCfdUpkXv6b5Tk1nZ6lfZrSmJWCN6s/tXEpINckoP2Ya3caskJEQuez1q+qHG4hmjBhOrbXs13t0+TLYvrtXMvZM2hXED/zz9rlquYFmF7Qs3q2PPo939fmNKU123sdFTOuVCUIe5Js9dCR6RpSqGuoFWGPv6xlW/9es074CAxx++svP6sH0c1D0+/3L78eE8P/4h1OtqS1J9v1sGvh1iCvVOVKZSuinRym4q/r7qyaclvN/7PqpuWEQRDOoI5l08MRrzMu5Ox6RWWbciP0ww3y1t1yZ29n/wDBjpD43QUAAA==',
          },
        },
      ],
    });
    expect(result).to.match(/inserted 1/i);
    expect(infos.length).to.equal(2);
    expect(infos[0].msg).to.equal('Event records');
    expect(infos[0].meta).to.eql({ raw: 1, decoded: 1 });
    expect(infos[1].msg).to.equal('Inserted 1 rows into dynamodb');
    expect(infos[1].meta).to.eql({ dest: 'dynamodb', rows: 1 });
  });
});
