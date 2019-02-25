'use strict';

require('./support');
const dynamo = require('../lib/dynamo');
const kinesis = require('../lib/kinesis');
const logger = require('../lib/logger');
const ByteDownloads = require('../lib/inputs/byte-downloads');

describe('byte-downloads', () => {

  it('recognizes bytes records', () => {
    const bytes = new ByteDownloads();
    expect(bytes.check({})).to.be.false;
    expect(bytes.check({type: 'anything'})).to.be.false;
    expect(bytes.check({type: 'bytes'})).to.be.true;
    expect(bytes.check({type: 'segmentbytes'})).to.be.true;
  });

  it('whitelists bytes preview/compliance records', () => {
    sinon.stub(logger, 'warn');
    const bytes = new ByteDownloads([{listenerSession: 'ls1', digest: 'd1', type: 'bytes'}]);
    const recs = bytes.filter([
      {num: 1, listenerSession: 'ls1', digest: 'd1', download: null, type: 'whatever'},
      {num: 2, listenerSession: 'ls1', digest: 'd1', download: {}, type: 'antebytespreview'},
      {num: 2, listenerSession: 'ls1', digest: 'd2', download: {}, type: 'antebytespreview'},
      {num: 3, listenerSession: 'ls1', digest: 'd1', download: {}, type: 'antebytes'},
      {num: 3, listenerSession: 'ls2', digest: 'd1', download: {}, type: 'antebytes'},
    ]);
    expect(recs.length).to.equal(2);
    expect(recs[0].num).to.equal(2);
    expect(recs[1].num).to.equal(3);
    expect(logger.warn).to.have.been.calledOnce;
    expect(logger.warn.args[0][0]).to.match(/unknown ddb record type/i);
  });

  it('changes the the record type from ante to post', () => {
    const bytes = new ByteDownloads([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 0, type: 'segmentbytes'},
    ]);
    const recs = bytes.filter([
      {
        listenerSession: 'ls1', digest: 'd1', type: 'antebytespreview',
        impressions: [{segment: 0, pings: ['ping', 'backs']}]
      },
      {
        listenerSession: 'ls1', digest: 'd1', type: 'antebytes',
        impressions: [{segment: 0, pings: ['ping', 'backs']}]
      },
    ]);

    expect(recs.length).to.equal(2);
    expect(recs[0].type).to.equal('postbytespreview');
    expect(recs[0].impressions[0].segment).to.equal(0);
    expect(recs[1].type).to.equal('postbytes');
    expect(recs[1].impressions[0].segment).to.equal(0);
  });

  it('whitelists which ddb records to queue', () => {
    const bytes = new ByteDownloads([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls2', digest: 'does-not-exist', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 2, type: 'segmentbytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 3, type: 'segmentbytes'},
      {listenerSession: 'ls2', digest: 'd2', segment: 0, type: 'segmentbytes'},
      {listenerSession: 'ls2', digest: 'd2', segment: 4, type: 'segmentbytes'},
    ]);
    const recs = bytes.filter([
      {
        type: 'antebytespreview', listenerSession: 'ls1', digest: 'd1', download: {},
        impressions: [{segment: 1}, {segment: 3}],
      },
      null,
      {
        type: 'antebytespreview', listenerSession: 'ls2', digest: 'd2', download: {},
        impressions: [{segment: 0}, {segment: 2}, {segment: 4}],
      },
      {
        type: 'antebytespreview', listenerSession: 'ls3', digest: 'd1', download: {},
        impressions: [{segment: 5}],
      },
    ]);

    expect(recs.length).to.equal(2);
    expect(recs[0].type).to.equal('postbytespreview');
    expect(recs[0].listenerSession).to.equal('ls1');
    expect(recs[0].digest).to.equal('d1');
    expect(recs[0].download).not.to.be.null;
    expect(recs[0].impressions.length).to.equal(1);
    expect(recs[0].impressions[0].segment).to.equal(3);
    expect(recs[1].type).to.equal('postbytespreview');
    expect(recs[1].listenerSession).to.equal('ls2');
    expect(recs[1].digest).to.equal('d2');
    expect(recs[1].download).to.be.null;
    expect(recs[1].impressions.length).to.equal(2);
    expect(recs[1].impressions[0].segment).to.equal(0);
    expect(recs[1].impressions[1].segment).to.equal(4);
  });

  it('decodes ids back to listenerSession + digest', async () => {
    const bytes = new ByteDownloads([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd2', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd3', type: 'bytes'},
    ]);
    sinon.stub(dynamo, 'get').callsFake(async () => [
      {id: 'ls1.d1', any: 'thing'},
      {id: 'ls1.d2', some: 'thing'},
      {foo: 'bar'},
    ]);

    const recs = await bytes.lookup();
    expect(recs.length).to.equal(3);
    expect(recs[0]).to.eql({listenerSession: 'ls1', digest: 'd1', any: 'thing'});
    expect(recs[1]).to.eql({listenerSession: 'ls1', digest: 'd2', some: 'thing'});
    expect(recs[2]).to.eql({foo: 'bar'});
  });

  it('inserts nothing', () => {
    return new ByteDownloads().insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts kinesis records', () => {
    let inserts = [];
    sinon.stub(kinesis, 'put').callsFake((recs) => {
      inserts = inserts.concat(recs);
      return Promise.resolve(recs.length);
    });

    // ddb.get returns values in order of keys
    sinon.stub(dynamo, 'get').callsFake(async () => [
      {
        type: 'antebytespreview', id: 'ls1.d1', download: {},
        impressions: [{segment: 1}, {segment: 3}],
      },
      null,
      {
        type: 'antebytes', id: 'ls2.d2', download: {},
        impressions: [{segment: 0}, {segment: 2}, {segment: 4}],
      },
    ]);

    const bytes = new ByteDownloads([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls2', digest: 'does-not-exist', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 2, type: 'segmentbytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 3, type: 'segmentbytes'},
      {listenerSession: 'ls2', digest: 'd2', segment: 0, type: 'segmentbytes'},
      {listenerSession: 'ls2', digest: 'd2', segment: 4, type: 'segmentbytes'},
    ]);

    return bytes.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('kinesis:foobar_stream');
      expect(result[0].count).to.equal(2);

      expect(inserts.length).to.equal(2);
      expect(inserts[0]).to.eql({
        type: 'postbytespreview',
        listenerSession: 'ls1',
        digest: 'd1',
        download: {},
        impressions: [{segment: 3}],
      });
      expect(inserts[1]).to.eql({
        type: 'postbytes',
        listenerSession: 'ls2',
        digest: 'd2',
        download: null,
        impressions: [{segment: 0}, {segment: 4}],
      });
    });
  });

  it('un-marks duplicate records', () => {
    let inserts = [];
    sinon.stub(kinesis, 'put').callsFake((recs) => {
      inserts = inserts.concat(recs);
      return Promise.resolve(recs.length);
    });

    // ddb.get returns values in order of keys
    sinon.stub(dynamo, 'get').resolves([{
      type: 'antebytespreview', id: 'ls1.d1', download: {isDuplicate: true, reason: 'bad'},
      impressions: [{segment: 1}, {segment: 3, isDuplicate: true, reason: 'bad'}],
    }]);

    const bytes = new ByteDownloads([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 1, type: 'segmentbytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 3, type: 'segmentbytes'},
    ]);

    return bytes.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('kinesis:foobar_stream');
      expect(result[0].count).to.equal(1);

      expect(inserts.length).to.equal(1);
      expect(inserts[0]).to.eql({
        type: 'postbytespreview',
        listenerSession: 'ls1',
        digest: 'd1',
        download: {},
        impressions: [{segment: 1}, {segment: 3}],
      });
    });
  });

});
