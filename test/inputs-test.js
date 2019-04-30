'use strict';

const support = require('./support');
const { BigqueryInputs, DynamoInputs, PingbackInputs, RedisInputs } = require('../lib/inputs');
const bigquery = require('../lib/bigquery');
const dynamo = require('../lib/dynamo');
const kinesis = require('../lib/kinesis');
const logger = require('../lib/logger');

describe('inputs', () => {

  it('handles unrecognized records', () => {
    let inputs = new BigqueryInputs([
      {type: 'combined', listenerEpisode: 'e1', timestamp: 0, download: {}},
      {type: 'foobar',   listenerEpisode: 'fb', timestamp: 0, download: {}},
      {type: 'combined', listenerEpisode: 'e2', timestamp: 0, download: {}},
      {what: 'ever'}
    ]);
    expect(inputs.unrecognized.length).to.equal(2);
    expect(inputs.unrecognized[0].listenerEpisode).to.equal('fb');
    expect(inputs.unrecognized[1].what).to.equal('ever');
  });

  it('inserts all bigquery inputs', () => {
    sinon.stub(logger, 'info');
    sinon.stub(bigquery, 'insert').callsFake((ds, tbl, rows) => Promise.resolve(rows.length));
    let inputs = new BigqueryInputs([
      {type: 'combined',     listenerId: 'i1', timestamp: 0, impressions: [{}]},
      {type: 'foobar',       listenerId: 'fb', timestamp: 0},
      {type: 'combined',     listenerId: 'd1', timestamp: 0, download: {}},
      {type: 'combined',     listenerId: 'i2', timestamp: 999999, impressions: [{}]},
      {type: 'segmentbytes', listenerId: 'b1', timestamp: 999999},
      {type: 'bytes',        listenerId: 'b2', timestamp: 999999}
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

  it('inserts dynamodb inputs', () => {
    sinon.stub(dynamo, 'write').callsFake(async (recs) => recs.length);
    sinon.stub(dynamo, 'get').callsFake(async () => [
      {id: 'le2.d2', type: 'antebytes', any: 'thing', download: {}, impressions: [
        {segment: 0, pings: ['ping', 'backs']},
      ]},
      {id: 'le3.d3', type: 'antebytes', what: 'ever', download: {}, impressions: [
        {segment: 0, pings: ['ping', 'backs']},
        {segment: 1, pings: ['ping', 'backs']},
        {segment: 2, pings: ['ping', 'backs']},
      ]},
    ]);
    sinon.stub(kinesis, 'put').callsFake(async (recs) => recs.length);
    sinon.stub(logger, 'info');

    let inputs = new DynamoInputs([
      {type: 'combined', listenerEpisode: 'le1', digest: 'd1'},
      {type: 'bytes', listenerEpisode: 'le2', digest: 'd2'},
      {type: 'segmentbytes', listenerEpisode: 'le3', digest: 'd3', segment: 2},
      {type: 'segmentbytes', listenerEpisode: 'le3', digest: 'd3', segment: 1},
      {type: 'antebytes', listenerEpisode: 'le4', digest: 'd4'},
      {type: 'antebytespreview', listenerEpisode: 'le5', digest: 'd5'},
    ]);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(2);
      expect(inserts.map(i => i.count)).to.eql([2, 3]);
      expect(inserts.map(i => i.dest).sort()).to.eql([
        'dynamodb',
        'kinesis:foobar_stream'
      ]);
    });
  });

  it('inserts pingback inputs', () => {
    sinon.stub(logger, 'info');
    nock('http://foo.bar').get('/i1').reply(200);
    nock('http://foo.bar').get('/i2').reply(200);
    let inputs = new PingbackInputs([
      {type: 'combined', listenerId: 'i1', timestamp: 0, impressions: [{pings: ['http://foo.bar/i1']}]},
      {type: 'foobar',   listenerId: 'fb', timestamp: 0},
      {type: 'combined', listenerId: 'd1', timestamp: 0, download: {}},
      {type: 'combined', listenerId: 'i2', timestamp: 999999, impressions: [
        {pings: ['http://bar.foo/{listener}'], isDuplicate: true},
        {pings: ['http://foo.bar/{listener}']}
      ]}
    ], true);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(1);
      expect(inserts[0].count).to.equal(2);
      expect(inserts[0].dest).to.equal('foo.bar');
    });
  });

  it('inserts redis increment inputs', () => {
    process.env.REDIS_HOST = 'whatev';
    sinon.stub(logger, 'info');
    let inputs = new RedisInputs([
      {type: 'combined', feederPodcast: null, timestamp: 0, download: {}},
      {type: 'foobar',   feederPodcast: 1, timestamp: 0},
      {type: 'combined', feederPodcast: 1, timestamp: 0, download: {}},
      {type: 'combined', feederPodcast: 1, timestamp: 999999}
    ], true);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(1);
      expect(inserts[0].count).to.equal(2);
      expect(inserts[0].dest).to.equal('redis://whatev');
    });
  });

});
