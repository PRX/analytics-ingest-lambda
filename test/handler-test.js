'use strict';

const support   = require('./support');
const testEvent = require('./support/test-event');
const bigquery  = require('../lib/bigquery');
const logger    = require('../lib/logger');
const index     = require('../index');
const handler   = index.handler;

describe('handler', () => {

  let inserted = {}, warns = [], errs = [];
  beforeEach(() => {
    warns = [], errs = [];
    sinon.stub(logger, 'error', msg => errs.push(msg));
    sinon.stub(logger, 'warn', msg => warns.push(msg));
    sinon.stub(logger, 'info');

    inserted = {};
    sinon.stub(bigquery, 'dataset', () => {
      return Promise.resolve({table: tbl => {
        return {insert: rows => {
          inserted[tbl] = rows;
          return Promise.resolve(rows.length);
        }};
      }});
    });
  });

  it('complains about insane inputs', done => {
    handler({foo: 'bar'}, null, (err, result) => {
      expect(err).to.be.undefined;
      expect(result).to.be.undefined;
      expect(errs.length).to.equal(1);
      expect(errs[0]).to.match(/invalid event input/i);
      done();
    });
  });

  it('complains about non-kinesis inputs', done => {
    handler({Records: [{}]}, null, (err, result) => {
      expect(err).to.be.undefined;
      expect(result).to.be.undefined;
      expect(errs.length).to.equal(1);
      expect(errs[0]).to.match(/invalid record input/i);
      done();
    });
  });

  it('complains about json parse errors', done => {
    handler({Records: [{kinesis: {data: 'not-json'}}]}, null, (err, result) => {
      expect(err).to.be.undefined;
      expect(result).to.be.undefined;
      expect(errs.length).to.equal(1);
      expect(errs[0]).to.match(/invalid record input/i);
      done();
    });
  });

  it('complains about unrecognized inputs', done => {
    let event = support.buildEvent(require('./support/test-records'));
    handler(event, null, (err, result) => {
      expect(errs.length).to.equal(1);
      expect(errs[0]).to.match(/unrecognized input record/i);
      done();
    });
  });

  it('handles kinesis records', done => {
    let event = support.buildEvent(require('./support/test-records'));
    handler(event, null, (err, result) => {
      expect(errs.length).to.equal(1);
      expect(result).to.match(/inserted 4/i);

      // based on test-records
      expect(inserted).to.have.keys(
        'dt_downloads$20170221',
        'dt_impressions$20170221',
        'dt_impressions$20170222'
      );

      expect(inserted['dt_downloads$20170221'].length).to.equal(1);
      expect(inserted['dt_downloads$20170221'][0].insertId).to.equal('req-uuid');
      let downloadJson = inserted['dt_downloads$20170221'][0].json;
      expect(downloadJson.digest).to.equal('the-digest');
      expect(downloadJson.program).to.equal('program-name');
      expect(downloadJson.path).to.equal('the/path/here');
      expect(downloadJson.feeder_podcast).to.equal(1234);
      expect(downloadJson.feeder_episode).to.equal('1234-5678');
      expect(downloadJson.remote_agent).to.equal('AppleCoreMedia/1.0.0.14B100 (iPhone; U; CPU OS 10_1_1 like Mac OS X; en_us)');
      expect(downloadJson.remote_ip).to.equal('24.49.134.194, 127.0.0.1, 99.99.99.99, whatev');
      expect(downloadJson.timestamp).to.equal(1487703699);
      expect(downloadJson.request_uuid).to.equal('req-uuid');
      expect(downloadJson.ad_count).to.equal(2);
      expect(downloadJson.is_duplicate).to.equal(false);
      expect(downloadJson.cause).to.be.null;
      expect(downloadJson.city_id).to.equal(5576882);
      expect(downloadJson.country_id).to.equal(6252001);
      expect(downloadJson.agent_name_id).to.equal(25);
      expect(downloadJson.agent_type_id).to.equal(36);
      expect(downloadJson.agent_os_id).to.equal(43);

      expect(inserted['dt_impressions$20170221'].length).to.equal(2);
      expect(inserted['dt_impressions$20170221'][0].insertId).not.to.equal('req-uuid');
      expect(inserted['dt_impressions$20170221'][1].insertId).not.to.equal('req-uuid');
      let impressionJson = inserted['dt_impressions$20170221'][0].json;
      expect(impressionJson.digest).to.equal('the-digest');
      expect(impressionJson.program).to.equal('program-name');
      expect(impressionJson.path).to.equal('the/path/here');
      expect(impressionJson.feeder_podcast).to.equal(1234);
      expect(impressionJson.feeder_episode).to.equal('1234-5678');
      expect(impressionJson.remote_agent).to.equal('curl/7.35.0');
      expect(impressionJson.remote_ip).to.equal('151.101.129.67');
      expect(impressionJson.timestamp).to.equal(1487703699);
      expect(impressionJson.request_uuid).to.equal('req-uuid');
      expect(impressionJson.ad_id).to.equal(12);
      expect(impressionJson.campaign_id).to.equal(34);
      expect(impressionJson.creative_id).to.equal(56);
      expect(impressionJson.flight_id).to.equal(78);
      expect(impressionJson.is_duplicate).to.equal(false);
      expect(impressionJson.cause).to.be.null;
      expect(impressionJson.city_id).to.equal(5391959);
      expect(impressionJson.country_id).to.equal(6252001);
      expect(impressionJson.agent_name_id).to.be.null;
      expect(impressionJson.agent_type_id).to.be.null;
      expect(impressionJson.agent_os_id).to.be.null;

      impressionJson = inserted['dt_impressions$20170221'][1].json;
      expect(impressionJson.ad_id).to.equal(98);
      expect(impressionJson.is_duplicate).to.equal(true);
      expect(impressionJson.cause).to.equal('something');
      expect(impressionJson.city_id).to.equal(4407066);
      expect(impressionJson.country_id).to.equal(6252001);
      expect(impressionJson.agent_name_id).to.be.null;
      expect(impressionJson.agent_type_id).to.be.null;
      expect(impressionJson.agent_os_id).to.be.null;

      impressionJson = inserted['dt_impressions$20170222'][0].json;
      expect(impressionJson.ad_id).to.equal(76);
      expect(impressionJson.is_duplicate).to.equal(false);
      expect(impressionJson.cause).to.equal(null);
      expect(impressionJson.city_id).to.be.null;
      expect(impressionJson.country_id).to.be.null;
      expect(impressionJson.agent_name_id).to.equal(21);
      expect(impressionJson.agent_type_id).to.equal(36);
      expect(impressionJson.agent_os_id).to.equal(42);

      done();
    });
  });

  it('handles pingback records', done => {
    process.env.PINGBACKS = 'true';
    let ping1 = nock('http://www.foo.bar').get('/ping1').reply(200);
    let ping2 = nock('http://www.foo.bar').get('/ping2').reply(404);
    let ping3 = nock('http://www.foo.bar').get('/ping3').reply(200);
    let ping4 = nock('http://www.foo.bar').get('/ping4').reply(200);

    let event = support.buildEvent(require('./support/test-records'));
    handler(event, null, (err, result) => {
      expect(errs.length).to.equal(1);
      expect(result).to.match(/inserted 2/i);

      expect(ping1.isDone()).to.be.true;
      expect(ping2.isDone()).to.be.true;
      expect(ping3.isDone()).to.be.false;
      expect(ping4.isDone()).to.be.true;

      expect(warns.length).to.equal(1);
      expect(warns[0]).to.match(/PINGFAIL error: http 404/i);
      expect(warns[0]).to.match(/ping2/);
      done();
    });
  });

  it('handles redis records', done => {
    process.env.REDIS_HOST = 'redis://127.0.0.1:6379';
    let event = support.buildEvent(require('./support/test-records'));
    handler(event, null, (err, result) => {
      expect(warns.length).to.equal(0);
      expect(errs.length).to.equal(1);
      expect(result).to.match(/inserted 12/i);

      let keys = [
        support.redisKeys('downloads.episodes.*'),
        support.redisKeys('downloads.podcasts.*'),
        support.redisKeys('impressions.episodes.*'),
        support.redisKeys('impressions.podcasts.*')
      ];
      Promise.all(keys).then(all => {
        expect(all[0].length).to.equal(2);
        expect(all[1].length).to.equal(2);
        expect(all[2].length).to.equal(4);
        expect(all[3].length).to.equal(4);
        done();
      });
    });
  });

});
