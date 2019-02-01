'use strict';

const support = require('./support');
const { BigqueryInputs, PingbackInputs, RedisInputs } = require('../lib/inputs');
const bigquery = require('../lib/bigquery');
const logger = require('../lib/logger');

describe('legacy-inputs', () => {

  it('handles unrecognized records', () => {
    let inputs = new BigqueryInputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0},
      {type: 'foobar',     requestUuid: 'fb', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 0},
      {what: 'ever'}
    ]);
    expect(inputs.unrecognized.length).to.equal(2);
    expect(inputs.unrecognized[0].requestUuid).to.equal('fb');
    expect(inputs.unrecognized[1].what).to.equal('ever');
  });

  it('inserts all bigquery inputs', () => {
    sinon.stub(logger, 'info');
    sinon.stub(bigquery, 'insert').callsFake((tbl, rows) => Promise.resolve(rows.length));
    let inputs = new BigqueryInputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0, impressionUrl: 'http://foo.bar'},
      {type: 'foobar',     requestUuid: 'fb', timestamp: 0},
      {type: 'download',   requestUuid: 'd1', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 999999},
      {type: 'segmentbytes', requestUuid: 'b1', timestamp: 999999},
      {type: 'bytes',        requestUuid: 'b2', timestamp: 999999}
    ]);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(2);
      expect(inserts.map(i => i.count)).to.eql([1, 2]);
      expect(inserts.map(i => i.dest).sort()).to.eql([
        'dt_downloads',
        'dt_impressions'
      ]);
    });
  });

  it('inserts adzerk impression inputs', () => {
    sinon.stub(logger, 'info');
    nock('http://foo.bar').get('/').reply(200);
    let inputs = new PingbackInputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0, impressionUrl: 'http://foo.bar'},
      {type: 'foobar',     requestUuid: 'fb', timestamp: 0},
      {type: 'download',   requestUuid: 'd1', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 999999}
    ], true);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(1);
      expect(inserts[0].count).to.equal(1);
      expect(inserts[0].dest).to.equal('foo.bar');
    });
  });

  it('inserts adzerk pingback inputs', () => {
    sinon.stub(logger, 'info');
    nock('http://foo.bar').get('/i1').reply(200);
    let inputs = new PingbackInputs([
      {requestUuid: 'i1', pingbacks: ['http://foo.bar/{uuid}']},
      {requestUuid: 'i2', pingbacks: ['http://bar.foo/{uuid}'], isDuplicate: true},
      {requestUuid: 'i3'}
    ], true);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(1);
      expect(inserts[0].count).to.equal(1);
      expect(inserts[0].dest).to.equal('foo.bar');
    });
  });

  it('inserts redis increment inputs', () => {
    process.env.REDIS_HOST = 'whatev';
    sinon.stub(logger, 'info');
    let inputs = new RedisInputs([
      {type: 'impression', feederPodcast: 1, timestamp: 0},
      {type: 'foobar',     feederPodcast: 1, timestamp: 0},
      {type: 'download',   feederPodcast: 1, timestamp: 0},
      {type: 'impression', feederPodcast: 1, timestamp: 999999}
    ], true);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(1);
      expect(inserts[0].count).to.equal(2);
      expect(inserts[0].dest).to.equal('redis://whatev');
    });
  });

});
