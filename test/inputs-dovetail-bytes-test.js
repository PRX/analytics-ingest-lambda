'use strict';

const support = require('./support');
const DovetailBytes = require('../lib/inputs/dovetail-bytes');

describe('dovetail-bytes', () => {

  it('recognizes bytes records', () => {
    const bytes = new DovetailBytes();
    expect(bytes.check({})).to.be.false;
    expect(bytes.check({type: 'anything'})).to.be.false;
    expect(bytes.check({type: 'bytes'})).to.be.true;
    expect(bytes.check({type: 'segmentbytes'})).to.be.true;
  });

  it('whitelists bytes preview/compliance records', () => {
    const bytes = new DovetailBytes([{listenerSession: 'ls1', digest: 'd1', type: 'bytes'}]);
    const recs = bytes.filter([
      {num: 1, listenerSession: 'ls1', digest: 'd1', download: {}},
      {num: 2, listenerSession: 'ls1', digest: 'd1', download: {}, bytesPreview: true},
      {num: 3, listenerSession: 'ls1', digest: 'd1', download: {}, bytesCompliance: true},
    ]);
    expect(recs.length).to.equal(2);
    expect(recs[0].num).to.equal(2);
    expect(recs[1].num).to.equal(3);
  });

  it('never marks a byte download as duplicate', () => {
    const bytes = new DovetailBytes([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 1, type: 'segmentbytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 3, type: 'segmentbytes'},
    ]);
    const recs = bytes.filter([
      {
        listenerSession: 'ls1',
        digest: 'd1',
        bytesPreview: true,
        download: {isDuplicate: true, cause: 'recentRequest', anything: 'else'},
        impressions: [
          {segment: 0, isDuplicate: true, cause: 'recentRequest'},
          {segment: 1, isDuplicate: true, cause: 'nonListener'},
          {segment: 2, isDuplicate: true, cause: 'noCampaign'},
          {segment: 3, isDuplicate: true, cause: 'anything'},
        ]
      },
    ]);

    expect(recs.length).to.equal(1);
    expect(recs[0].type).to.equal('combinedbytes');
    expect(recs[0].listenerSession).to.equal('ls1');
    expect(recs[0].digest).to.equal('d1');
    expect(recs[0].download).to.eql({isDuplicate: false, cause: null, anything: 'else'});
    expect(recs[0].impressions[0]).to.eql({segment: 1, isDuplicate: false, cause: null, pings: []});
    expect(recs[0].impressions[1]).to.eql({segment: 3, isDuplicate: false, cause: null, pings: []});
  });

  it('only runs pingbacks in compliance mode', () => {
    const bytes = new DovetailBytes([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 0, type: 'segmentbytes'},
    ]);
    const recs = bytes.filter([
      {listenerSession: 'ls1', digest: 'd1', bytesPreview: true,
        impressions: [{segment: 0, pings: ['ping', 'backs']}]},
      {listenerSession: 'ls1', digest: 'd1', bytesCompliance: true,
        impressions: [{segment: 0, pings: ['ping', 'backs']}]},
    ]);

    expect(recs.length).to.equal(2);
    expect(recs[0].impressions[0].segment).to.equal(0);
    expect(recs[0].impressions[0].pings).to.eql([]);
    expect(recs[1].impressions[0].segment).to.equal(0);
    expect(recs[1].impressions[0].pings).to.eql(['ping', 'backs']);
  });

  it('whitelists which ddb records to queue', () => {
    const bytes = new DovetailBytes([
      {listenerSession: 'ls1', digest: 'd1', type: 'bytes'},
      {listenerSession: 'ls2', digest: 'does-not-exist', type: 'bytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 2, type: 'segmentbytes'},
      {listenerSession: 'ls1', digest: 'd1', segment: 3, type: 'segmentbytes'},
      {listenerSession: 'ls2', digest: 'd2', segment: 0, type: 'segmentbytes'},
      {listenerSession: 'ls2', digest: 'd2', segment: 4, type: 'segmentbytes'},
    ]);
    const recs = bytes.filter([
      {bytesPreview: true, listenerSession: 'ls1', digest: 'd1', download: {}, impressions: [
        {segment: 1},
        {segment: 3},
      ]},
      {bytesPreview: true, listenerSession: 'ls2', digest: 'd2', download: {}, impressions: [
        {segment: 0},
        {segment: 2},
        {segment: 4},
      ]},
      {bytesPreview: true, listenerSession: 'ls3', digest: 'd1', download: {}, impressions: [{segment: 5}]},
    ]);

    expect(recs.length).to.equal(2);
    expect(recs[0].type).to.equal('combinedbytes');
    expect(recs[0].listenerSession).to.equal('ls1');
    expect(recs[0].digest).to.equal('d1');
    expect(recs[0].bytesPreview).to.equal(true);
    expect(recs[0].download).not.to.be.null;
    expect(recs[0].impressions.length).to.equal(1);
    expect(recs[0].impressions[0].segment).to.equal(3);
    expect(recs[1].type).to.equal('combinedbytes');
    expect(recs[1].listenerSession).to.equal('ls2');
    expect(recs[1].digest).to.equal('d2');
    expect(recs[1].bytesPreview).to.equal(true);
    expect(recs[1].download).to.be.null;
    expect(recs[1].impressions.length).to.equal(2);
    expect(recs[1].impressions[0].segment).to.equal(0);
    expect(recs[1].impressions[1].segment).to.equal(4);
  });

  it('inserts nothing', () => {
    return new DovetailBytes().insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  xit('inserts byte records', () => {
    let inserts = {};
    sinon.stub(bigquery, 'insert').callsFake((tbl, rows) => {
      inserts[tbl] = rows;
      return Promise.resolve(rows.length);
    });
    const bytes2 = new DovetailBytes([
      {type: 'bytes', request_uuid: 'the-uuid1', timestamp: 1490827132999},
      {type: 'segmentbytes', request_uuid: 'the-uuid2', segment_index: 2, timestamp: 1490827132999},
      {type: 'bytes', request_uuid: 'the-uuid3', timestamp: 1490837132},
      {type: 'download', request_uuid: 'the-uuid4', timestamp: 1490827132999},
    ]);
    return bytes2.insert().then(result => {
      expect(result.length).to.equal(2);
      expect(result[0].dest).to.equal('dt_download_bytes');
      expect(result[0].count).to.equal(2);
      expect(result[1].dest).to.equal('dt_impression_bytes');
      expect(result[1].count).to.equal(1);
      expect(inserts['dt_download_bytes'].length).to.equal(2);
      expect(inserts['dt_download_bytes'][0].json.request_uuid).to.equal('the-uuid1');
      expect(inserts['dt_download_bytes'][1].json.request_uuid).to.equal('the-uuid3');
      expect(inserts['dt_impression_bytes'].length).to.equal(1);
      expect(inserts['dt_impression_bytes'][0].json.request_uuid).to.equal('the-uuid2');
    });
  });

});
