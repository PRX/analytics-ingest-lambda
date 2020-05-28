'use strict';

require('./support');
const dynamo = require('../lib/dynamo');
const kinesis = require('../lib/kinesis');
const logger = require('../lib/logger');
const ByteDownloads = require('../lib/inputs/byte-downloads');

describe('byte-downloads', () => {

  let bytes, originalRecord;
  beforeEach(() => {
    bytes = new ByteDownloads();
    originalRecord = {
      type: 'antebytes',
      download: {adCount: 3},
      impressions: [
        {segment: 1, adId: 11, campaignId: 12, creativeId: 13, flightId: 14, pings: ['pings1']},
        {segment: 2, adId: 21, campaignId: 22, creativeId: 23, flightId: 24, pings: ['pings2']},
        {segment: 3, adId: 31, campaignId: 32, creativeId: 33, flightId: 34, pings: ['pings3']},
      ],
      requestUuid: 'the-request',
      digest: 'the-digest',
      feederPodcast: 'the-podcast',
      feederEpisode: 'the-episode',
      listenerId: 'the-listener-id',
      listenerEpisode: 'the-listener-episode',
      remoteAgent: 'the-agent',
      remoteIp: 'the-ip',
      remoteReferrer: 'the-referer',
      timestamp: 99999999999,
      url: 'the-url',
      anything: 'else',
    };
  });

  it('recognizes bytes records', () => {
    expect(bytes.check({})).to.be.false;
    expect(bytes.check({type: 'anything'})).to.be.false;
    expect(bytes.check({type: 'bytes'})).to.be.true;
    expect(bytes.check({type: 'segmentbytes'})).to.be.true;
  });

  it('formats bad original records', () => {
    sinon.stub(logger, 'warn');

    expect(bytes.format({type: 'bytes', listenerEpisode: 'le', digest: 'd'}, null)).to.be.null;
    expect(bytes.format({type: 'segmentbytes', segment: 99}, originalRecord)).to.be.null;
    expect(bytes.format({type: 'segmentbytes', segment: null}, originalRecord)).to.be.null;

    expect(bytes.format({type: 'bytes'}, {type: 'unknown'})).to.be.null;
    expect(logger.warn).to.have.callCount(1);
    expect(logger.warn.args[0][0]).to.match(/unknown ddb record type/i);
  });

  it('formats record post-bytes types', () => {
    expect(bytes.format({type: 'bytes'}, originalRecord).type).to.equal('postbytes');
    expect(bytes.format({type: 'bytes'}, {...originalRecord, type: 'antebytespreview'}).type).to.equal('postbytespreview');
    expect(bytes.format({type: 'segmentbytes', segment: 1}, originalRecord).type).to.equal('postbytes');
    expect(bytes.format({type: 'segmentbytes', segment: 1}, {...originalRecord, type: 'antebytespreview'}).type).to.equal('postbytespreview');
  });

  it('formats record timestamps to when the bytes downloaded', () => {
    expect(bytes.format({type: 'bytes', timestamp: 1234}, originalRecord).timestamp).to.equal(1234);
    expect(bytes.format({type: 'bytes', timestamp: null}, originalRecord).timestamp).not.to.equal(5678);
    expect(bytes.format({type: 'segmentbytes', segment: 1, timestamp: 1234}, originalRecord).timestamp).to.equal(1234);
  });

  it('formats duplicate causes', () => {
    let formatted = bytes.format({type: 'bytes', isDuplicate: true, cause: 'bad'}, originalRecord);
    expect(formatted.download.isDuplicate).to.equal(true);
    expect(formatted.download.cause).to.equal('bad');

    formatted = bytes.format({type: 'bytes'}, originalRecord);
    expect(formatted.download.isDuplicate).to.equal(false);
    expect(formatted.download.cause).to.be.null;

    formatted = bytes.format({type: 'segmentbytes', segment: 1, isDuplicate: true, cause: 'bad'}, originalRecord);
    expect(formatted.impressions[0].isDuplicate).to.equal(true);
    expect(formatted.impressions[0].cause).to.equal('bad');

    formatted = bytes.format({type: 'segmentbytes', segment: 1}, originalRecord);
    expect(formatted.impressions[0].isDuplicate).to.equal(false);
    expect(formatted.impressions[0].cause).to.be.null;
  });

  it('formats bytes download records', () => {
    const rec = {type: 'bytes', timestamp: 1234, listenerEpisode: 'the-listener-episode',
                 digest: 'the-digest', bytes: 9999, seconds: 12.34, percent: 0.65};
    const formatted = bytes.format(rec, originalRecord);
    expect(formatted).to.eql({
      anything: 'else',
      digest: 'the-digest',
      download: {adCount: 3, cause: null, isDuplicate: false},
      feederEpisode: 'the-episode',
      feederPodcast: 'the-podcast',
      impressions: [],
      listenerEpisode: 'the-listener-episode',
      listenerId: 'the-listener-id',
      remoteAgent: 'the-agent',
      remoteIp: 'the-ip',
      remoteReferrer: 'the-referer',
      requestUuid: 'the-request',
      timestamp: 1234,
      type: 'postbytes',
      url: 'the-url',
    });
  });

  it('formats segmentbytes impression records', () => {
    const rec = {type: 'segmentbytes', timestamp: 1234, listenerEpisode: 'the-listener-episode',
                 digest: 'the-digest', segment: 2};
    const formatted = bytes.format(rec, originalRecord);
    expect(formatted).to.eql({
      digest: 'the-digest',
      download: null,
      feederEpisode: 'the-episode',
      feederPodcast: 'the-podcast',
      impressions: [{
        adId: 21,
        campaignId: 22,
        cause: null,
        creativeId: 23,
        flightId: 24,
        isDuplicate: false,
        pings: ['pings2'],
        segment: 2,
      }],
      listenerEpisode: 'the-listener-episode',
      listenerId: 'the-listener-id',
      remoteAgent: 'the-agent',
      remoteIp: 'the-ip',
      remoteReferrer: 'the-referer',
      requestUuid: 'the-request',
      timestamp: 1234,
      type: 'postbytes',
      url: 'the-url',
    });
  });

  it('formats retry records', () => {
    sinon.stub(Date, 'now').returns(9999);
    expect(bytes.formatRetry({})).to.eql({retryCount: 1, retryAt: 9999});
    expect(bytes.formatRetry({retryCount: 2})).to.eql({retryCount: 3, retryAt: 9999});
    expect(bytes.formatRetry({retryAt: 8888})).to.eql({retryCount: 1, retryAt: 8888});
    expect(bytes.formatRetry({retryCount: 2, retryAt: 8888})).to.eql({retryCount: 3, retryAt: 8888});
  });

  it('checks if records are retryable', () => {
    sinon.stub(Date, 'now').returns(400000);
    expect(bytes.shouldRetry({})).to.equal(true);
    expect(bytes.shouldRetry({retryCount: 19})).to.equal(true);
    expect(bytes.shouldRetry({retryCount: 20})).to.equal(false);
    expect(bytes.shouldRetry({retryAt: 100000})).to.equal(true);
    expect(bytes.shouldRetry({retryAt: 99999})).to.equal(false);
  });

  it('looks up dynamodb records', async () => {
    const bytes = new ByteDownloads([
      {listenerEpisode: 'le1', digest: 'd1', type: 'bytes'},
      {listenerEpisode: 'le1', digest: 'd2', type: 'bytes'},
      {listenerEpisode: 'le1', digest: 'd3', type: 'bytes'},
    ]);
    sinon.stub(dynamo, 'get').callsFake(async () => [
      {id: 'le1.d1', any: 'thing'},
      {id: 'le1.d2', some: 'thing'},
      {foo: 'bar'},
    ]);

    const recs = await bytes.lookup();
    expect(dynamo.get).to.have.been.calledOnce;
    expect(dynamo.get.args[0][0]).to.eql(['le1.d1', 'le1.d2', 'le1.d3']);
    expect(Object.keys(recs)).to.eql(['le1.d1', 'le1.d2']);
    expect(recs['le1.d1']).to.eql({listenerEpisode: 'le1', digest: 'd1', any: 'thing'});
    expect(recs['le1.d2']).to.eql({listenerEpisode: 'le1', digest: 'd2', some: 'thing'});
  });

  it('inserts nothing', () => {
    return new ByteDownloads().insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts kinesis records', () => {
    let inserts = [], retries = [];
    sinon.stub(kinesis, 'put').callsFake((recs) => {
      inserts = inserts.concat(recs);
      return Promise.resolve(recs.length);
    });
    sinon.stub(kinesis, 'putRetry').callsFake((recs) => {
      retries = retries.concat(recs);
      return Promise.resolve(recs.length);
    });

    sinon.stub(logger, 'warn');
    sinon.stub(dynamo, 'get').callsFake(async () => [
      {...originalRecord, type: 'antebytespreview', id: 'le1.d1'},
      null,
      {...originalRecord, type: 'antebytes', id: 'le2.d2'},
    ]);

    const bytes = new ByteDownloads([
      {listenerEpisode: 'le1', digest: 'd1', type: 'bytes', timestamp: 1234},
      {listenerEpisode: 'le1', digest: 'd1', type: 'bytes', timestamp: 5678, isDuplicate: true},
      {listenerEpisode: 'le1', digest: 'd1', segment: 2, type: 'segmentbytes'},
      {listenerEpisode: 'le1', digest: 'd1', segment: 0, type: 'segmentbytes'},
      {listenerEpisode: 'le2', digest: 'd2', segment: 3, type: 'segmentbytes'},
      {listenerEpisode: 'le2', digest: 'd2', segment: 4, type: 'segmentbytes'},
      // ignored segment not in the DDB record
      {listenerEpisode: 'le2', digest: 'd2', segment: 999, type: 'segmentbytes'},
      // le.digests not found in DDB (retry candidates)
      {type: 'bytes', listenerEpisode: 'le3', digest: 'brand-new'},
      {type: 'bytes', listenerEpisode: 'le3', digest: 'still-retryable', retryCount: 2, retryAt: 9999999999999},
      {type: 'bytes', listenerEpisode: 'le3', digest: 'out-of-retries', retryCount: 999},
      {type: 'bytes', listenerEpisode: 'le3', digest: 'out-of-time', retryAt: 1000},
    ]);

    return bytes.insert().then(result => {
      expect(result.length).to.equal(2);
      expect(result[0].dest).to.equal('kinesis:foobar_stream');
      expect(result[0].count).to.equal(4);
      expect(result[1].dest).to.equal('kinesis:foobar_retry_stream');
      expect(result[1].count).to.equal(2);

      expect(inserts.length).to.equal(4);

      expect(inserts[0].type).to.equal('postbytespreview');
      expect(inserts[0].listenerEpisode).to.equal('le1');
      expect(inserts[0].digest).to.equal('d1');
      expect(inserts[0].timestamp).to.equal(1234);
      expect(inserts[0].download.isDuplicate).to.equal(false);
      expect(inserts[0].impressions).to.eql([]);

      expect(inserts[1].type).to.equal('postbytespreview');
      expect(inserts[1].listenerEpisode).to.equal('le1');
      expect(inserts[1].digest).to.equal('d1');
      expect(inserts[1].timestamp).to.equal(5678);
      expect(inserts[1].download.isDuplicate).to.equal(true);
      expect(inserts[1].impressions).to.eql([]);

      expect(inserts[2].type).to.equal('postbytespreview');
      expect(inserts[2].listenerEpisode).to.equal('le1');
      expect(inserts[2].digest).to.equal('d1');
      expect(inserts[2].download).to.be.null;
      expect(inserts[2].impressions.length).to.eql(1);
      expect(inserts[2].impressions[0].segment).to.eql(2);
      expect(inserts[2].impressions[0].adId).to.eql(21);

      expect(inserts[3].type).to.equal('postbytes');
      expect(inserts[3].listenerEpisode).to.equal('le2');
      expect(inserts[3].digest).to.equal('d2');
      expect(inserts[3].download).to.be.null;
      expect(inserts[3].impressions.length).to.eql(1);
      expect(inserts[3].impressions[0].segment).to.eql(3);
      expect(inserts[3].impressions[0].adId).to.eql(31);

      expect(retries.length).to.equal(2);

      expect(retries[0].type).to.equal('bytes');
      expect(retries[0].digest).to.equal('brand-new');
      expect(retries[0].retryCount).to.equal(1);
      expect(retries[0].retryAt).to.be.at.most(new Date().getTime());
      expect(retries[0].retryAt).to.be.at.least(new Date().getTime() - 10);

      expect(retries[1].type).to.equal('bytes');
      expect(retries[1].digest).to.equal('still-retryable');
      expect(retries[1].retryCount).to.equal(3);
      expect(retries[1].retryAt).to.equal(9999999999999);

      expect(logger.warn).to.have.callCount(4);
      expect(logger.warn.args[0][0]).to.equal('DDB retrying le3.brand-new');
      expect(logger.warn.args[0][1]).to.eql({ddb: 'retrying', count: 1});
      expect(logger.warn.args[1][0]).to.equal('DDB retrying le3.still-retryable');
      expect(logger.warn.args[1][1]).to.eql({ddb: 'retrying', count: 3});
      expect(logger.warn.args[2][0]).to.equal('DDB missing le3.out-of-retries');
      expect(logger.warn.args[2][1]).to.eql({ddb: 'missing'});
      expect(logger.warn.args[3][0]).to.equal('DDB missing le3.out-of-time');
      expect(logger.warn.args[3][1]).to.eql({ddb: 'missing'});
    });
  });

});
