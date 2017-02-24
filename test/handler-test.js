'use strict';

const helper    = require('./support');
const testEvent = require('./support/test-event');
const bigquery  = require('../lib/bigquery');
const index     = require('../index');
const handler   = index.handler;

describe('handler', () => {

  let inserted = {};
  before(() => {
    sinon.stub(index, 'logSuccess');
    sinon.stub(index, 'logError');

    inserted = {};
    sinon.stub(bigquery, 'dataset', () => {
      return {table: tbl => {
        return {insert: rows => {
          inserted[tbl] = rows;
          return Promise.resolve(rows.length);
        }};
      }};
    });
  });

  it('rejects insane inputs', done => {
    handler({foo: 'bar'}, null, (err, result) => {
      expect(err).to.be.an.instanceof(Error);
      expect(err).to.match(/invalid event input/i);
      done();
    });
  });

  it('ignores non-kinesis inputs', done => {
    handler({Records: [{}]}, null, (err, result) => {
      expect(err).to.be.null;
      expect(result).to.match(/inserted 0/i);
      done();
    });
  });

  it('handles the test event', done => {
    handler(testEvent, null, (err, result) => {
      expect(err).to.be.null;
      expect(result).to.match(/inserted 3/i);

      // based on test event contents
      expect(inserted).to.have.keys('foobar_table');
      expect(inserted.foobar_table.length).to.equal(3);
      expect(inserted.foobar_table[0].insertId).to.equal('2ca8dc50-f868-11e6-86f9-bb1c46cbfd78');
      expect(inserted.foobar_table[0].json).to.eql({
        ad_id: 2293393,
        campaign_id: 534851,
        creative_id: 1899685,
        flight_id: 2725271,
        path: 'a43d8e6c-b9c5-4855-b1f6-03bbfd368a0a/moth_431_mrh_1621_4_19_16.mp3',
        program: 'themoth',
        remote_agent: 'gPodder/3.8.3 (+http://gpodder.org/)',
        remote_ip: '75.132.24.235',
        timestamp: 1487703699000,
        impression_sent: false,
        request_uuid: '2ca8dc50-f868-11e6-86f9-bb1c46cbfd78',
        is_duplicate: true
      });
      done();
    });
  });

});
