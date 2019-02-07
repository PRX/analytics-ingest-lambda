'use strict';

const support = require('./support');
const bigquery = require('../lib/bigquery');
const DovetailBytes = require('../lib/inputs/dovetail-bytes');

describe('dovetail-bytes', () => {

  let bytes = new DovetailBytes();

  it('recognizes bytes records', () => {
    expect(bytes.check({})).to.be.false;
    expect(bytes.check({type: 'anything'})).to.be.false;
    expect(bytes.check({type: 'bytes'})).to.be.true;
    expect(bytes.check({type: 'segmentbytes'})).to.be.true;
  });

  it('formats download byte inserts', () => {
    const record = bytes.formatDownload({
      timestamp: 1490827132999,
      request_uuid: 'the-uuid',
      bytes_downloaded: 123,
      seconds_downloaded: 1.23,
      percent_downloaded: 0.12,
    })
    expect(record).to.have.keys('insertId', 'json');
    expect(record.insertId).to.equal('bytes-the-uuid');
    expect(record.json).to.have.keys('timestamp', 'request_uuid', 'bytes', 'seconds', 'percent');
    expect(record.json.timestamp).to.equal(1490827132);
    expect(record.json.request_uuid).to.equal('the-uuid');
    expect(record.json.bytes).to.equal(123);
    expect(record.json.seconds).to.equal(1.23);
    expect(record.json.percent).to.equal(0.12);
  });

  it('formats impression byte inserts', () => {
    const record = bytes.formatImpression({
      timestamp: 1490827132999,
      request_uuid: 'the-uuid',
      segment_index: 2,
      bytes_downloaded: 123,
      seconds_downloaded: 1.23,
      percent_downloaded: 0.12,
    })
    expect(record).to.have.keys('insertId', 'json');
    expect(record.insertId).to.equal('bytes-the-uuid-2');
    expect(record.json).to.have.keys('timestamp', 'request_uuid', 'segment_index', 'bytes', 'seconds', 'percent');
    expect(record.json.timestamp).to.equal(1490827132);
    expect(record.json.request_uuid).to.equal('the-uuid');
    expect(record.json.segment_index).to.equal(2);
    expect(record.json.bytes).to.equal(123);
    expect(record.json.seconds).to.equal(1.23);
    expect(record.json.percent).to.equal(0.12);
  });

  it('inserts nothing', () => {
    return bytes.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts byte records', () => {
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
