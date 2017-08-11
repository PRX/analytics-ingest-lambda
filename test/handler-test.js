'use strict';

const support   = require('./support');
const testEvent = require('./support/test-event');
const bigquery  = require('../lib/bigquery');
const index     = require('../index');
const handler   = index.handler;

describe('handler', () => {

  let inserted = {};
  beforeEach(() => {
    sinon.stub(index, 'logSuccess');
    sinon.stub(index, 'logError');

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

  it('rejects insane inputs', done => {
    handler({foo: 'bar'}, null, (err, result) => {
      expect(err).to.be.an.instanceof(Error);
      expect(err).to.match(/invalid event input/i);
      done();
    });
  });

  it('rejects non-kinesis inputs but still runs the insert', done => {
    handler({Records: [{}]}, null, (err, result) => {
      expect(err).to.be.an.instanceof(Error);
      expect(err).to.match(/invalid record input/i);
      expect(result).to.match(/inserted 0/i);
      done();
    });
  });

  it('handles kinesis records', done => {
    let event = support.buildEvent(require('./support/test-records'));
    handler(event, null, (err, result) => {
      expect(err).to.be.an.instanceof(Error);
      expect(err).to.match(/unrecognized input record/i);
      expect(result).to.match(/inserted 4/i);

      // based on test-records
      expect(inserted).to.have.keys(
        'the_downloads_table$20170221',
        'the_impressions_table$20170221',
        'the_impressions_table$20170222'
      );

      expect(inserted['the_downloads_table$20170221'].length).to.equal(1);
      expect(inserted['the_downloads_table$20170221'][0].insertId).to.equal('req-uuid');
      let downloadJson = inserted['the_downloads_table$20170221'][0].json;
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

      expect(inserted['the_impressions_table$20170221'].length).to.equal(2);
      expect(inserted['the_impressions_table$20170221'][0].insertId).not.to.equal('req-uuid');
      expect(inserted['the_impressions_table$20170221'][1].insertId).not.to.equal('req-uuid');
      let impressionJson = inserted['the_impressions_table$20170221'][0].json;
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

      impressionJson = inserted['the_impressions_table$20170221'][1].json;
      expect(impressionJson.ad_id).to.equal(98);
      expect(impressionJson.is_duplicate).to.equal(true);
      expect(impressionJson.cause).to.equal('something');
      expect(impressionJson.city_id).to.equal(4407066);
      expect(impressionJson.country_id).to.equal(6252001);
      expect(impressionJson.agent_name_id).to.be.null;
      expect(impressionJson.agent_type_id).to.be.null;
      expect(impressionJson.agent_os_id).to.be.null;

      impressionJson = inserted['the_impressions_table$20170222'][0].json;
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

});
