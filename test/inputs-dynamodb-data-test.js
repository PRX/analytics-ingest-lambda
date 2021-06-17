require('./support');

const dynamo = require('../lib/dynamo');
const kinesis = require('../lib/kinesis');
const logger = require('../lib/logger');
const DynamodbData = require('../lib/inputs/dynamodb-data');

describe('dynamodb-data', () => {
  it('organizes records into payloads and segments', () => {
    const recs = [
      { listenerEpisode: 'le1', digest: 'd1', timestamp: 1000, type: 'antebytes', any: 'data' },
      { listenerEpisode: 'le1', digest: 'd1', timestamp: 2000, type: 'bytes' },
      { listenerEpisode: 'le2', digest: 'd1', timestamp: 3000, type: 'bytes' },
      { listenerEpisode: 'le1', digest: 'd1', timestamp: 4000, type: 'segmentbytes', segment: 2 },
    ];
    const ddb = new DynamodbData(recs);

    expect(ddb.payloads).to.eql({
      'le1.d1': { timestamp: 1000, type: 'antebytes', any: 'data' },
    });
    expect(ddb.segments).to.eql({
      'le1.d1': ['2000', '4000.2'],
      'le2.d1': ['3000'],
    });
  });

  it('recognizes antebytes and bytes records', () => {
    const ddb = new DynamodbData();

    expect(ddb.check({})).to.be.false;
    expect(ddb.check({ type: 'combined' })).to.be.false;
    expect(ddb.check({ type: 'postbytes' })).to.be.false;

    expect(ddb.check({ type: 'antebytes' })).to.be.true;
    expect(ddb.check({ type: 'antebytespreview' })).to.be.true;
    expect(ddb.check({ type: 'bytes' })).to.be.true;
    expect(ddb.check({ type: 'segmentbytes' })).to.be.true;
  });

  it('encodes payload keys', () => {
    const ddb = new DynamodbData();
    const data = { listenerEpisode: 'a', digest: 'b' };

    expect(ddb.encodeKey(data)).to.eql(['a.b', {}]);
    expect(ddb.encodeKey({ any: [9, 'data'], ...data })).to.eql(['a.b', { any: [9, 'data'] }]);
  });

  it('decodes payload keys', () => {
    const ddb = new DynamodbData();
    const data = { any: [9, 'data'] };

    expect(ddb.decodeKey('a.b', data)).to.eql({ listenerEpisode: 'a', digest: 'b', ...data });
    expect(ddb.decodeKey('a.b.c', data)).to.eql({ listenerEpisode: 'a', digest: 'b.c', ...data });
  });

  it('encodes segments', () => {
    const ddb = new DynamodbData();

    expect(ddb.encodeSegment({ type: 'bytes', timestamp: 99 })).to.equal('99');
    expect(ddb.encodeSegment({ type: 'bytes' })).to.match(/^[0-9]+$/);
    expect(ddb.encodeSegment({ timestamp: 99 })).to.equal('99');
    expect(ddb.encodeSegment({})).to.match(/^[0-9]+$/);

    expect(ddb.encodeSegment({ type: 'segmentbytes', timestamp: 99, segment: 4 })).to.equal('99.4');
    expect(ddb.encodeSegment({ type: 'segmentbytes', segment: 4 })).to.match(/^[0-9]+\.4$/);
  });

  it('decodes segments', () => {
    const ddb = new DynamodbData();
    const isoString = '2021-06-17T18:20:03.879Z';
    const epochMs = Date.parse(isoString);

    expect(ddb.decodeSegment(`${epochMs}`)).to.eql(['20210617', epochMs, 'DOWNLOAD']);
    expect(ddb.decodeSegment(`${epochMs}.4`)).to.eql(['20210617', epochMs, 4]);
    expect(ddb.decodeSegment(`${epochMs}.whatev`)).to.eql(['20210617', epochMs, 'whatev']);
  });

  describe('#insert', () => {
    it('inserts into kinesis', async () => {});

    it('throws an error on any ddb failures', async () => {});
  });

  describe('#format', () => {
    const ddb = new DynamodbData();

    it('handles empty inputs', () => {
      expect(ddb.format(['le.d', null, { 1: true }])).to.eql([]);
      expect(ddb.format(['le.d', {}, null])).to.eql([]);
      expect(ddb.format(['le.d', {}, {}])).to.eql([]);
    });

    it('formats postbyte records', () => {
      const segments = { 1000: true, 100000: true, 200000: false };
      const records = ddb.format(['le.d', { some: 'data' }, segments]);

      expect(records.length).to.equal(2);
      expect(records[0]).to.eql({
        type: 'postbytes',
        listenerEpisode: 'le',
        digest: 'd',
        timestamp: 1000,
        some: 'data',
      });
      expect(records[1]).to.eql({
        type: 'postbytes',
        listenerEpisode: 'le',
        digest: 'd',
        timestamp: 100000,
        some: 'data',
      });
    });

    it('filters downloads', () => {
      const download = { the: 'download' };
      const segments = {
        1000: true,
        100000: false,
        100000.1: true,
        200000: true,
        200000.2: true,
      };
      const records = ddb.format(['le.d', { download }, segments]);

      expect(records.length).to.equal(3);
      expect(records[0].download).to.eql(download);
      expect(records[1].download).to.be.undefined;
      expect(records[2].download).to.eql(download);
    });

    it('filters impressions', () => {
      const impressions = [
        { segment: 1, num: 'imp1' },
        { segment: 3, num: 'imp3' },
        { segment: 2, num: 'imp2' },
      ];
      const segments = {
        1000: true,
        100000.2: true,
        100001.1: false,
        100002.1: true,
        200000.1: true,
        200001.2: false,
        200002.3: true,
      };
      const records = ddb.format(['le.d', { impressions }, segments]);

      expect(records.length).to.equal(3);
      expect(records[0].timestamp).to.equal(1000);
      expect(records[0].impressions).to.eql([]);
      expect(records[1].timestamp).to.equal(100000);
      expect(records[1].impressions).to.eql([impressions[2]]);
      expect(records[2].timestamp).to.equal(200002);
      expect(records[2].impressions).to.eql([impressions[0], impressions[1]]);
    });
  });

  describe('#dedupSegments', () => {
    const ddb = new DynamodbData();

    it('filters old segments', () => {
      const [segments] = ddb.dedupSegments({
        1000: true,
        1000.1: false,
        1000.2: false,
        1000.3: true,
        1000.4: true,
      });
      expect(segments).to.eql({ 19700101: ['DOWNLOAD', 3, 4] });
    });

    it('returns the max timestamp per segment per day', () => {
      const [, timestamps] = ddb.dedupSegments({
        1000: true,
        3000.1: true,
        5000.2: true,
        2000.3: true,
        1000.4: true,
      });
      expect(timestamps).to.eql({ 19700101: 5000 });
    });

    it('dedups segments by utc day', () => {
      const [segments, timestamps] = ddb.dedupSegments({
        1000: true,
        2000: false,
        2000.4: false,
        100000: true,
        100000.2: false,
        100001.2: true,
        200001.2: true,
        200000.4: true,
      });
      expect(segments).to.eql({ 19700102: ['DOWNLOAD'], 19700103: [2, 4] });
      expect(timestamps).to.eql({ 19700102: 100000, 19700103: 200001 });
    });
  });
});
