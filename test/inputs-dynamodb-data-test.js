require('./support');

const dynamo = require('../lib/dynamo');
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

  it('dedups segments', () => {
    const recs = [
      { listenerEpisode: 'le1', digest: 'd1', timestamp: 2000, type: 'bytes' },
      { listenerEpisode: 'le1', digest: 'd1', timestamp: 2000, type: 'bytes' },
      { listenerEpisode: 'le1', digest: 'd1', timestamp: 4000, type: 'segmentbytes', segment: 2 },
      { listenerEpisode: 'le1', digest: 'd1', timestamp: 4000, type: 'segmentbytes', segment: 2 },
    ];
    const ddb = new DynamodbData(recs);

    expect(ddb.segments).to.eql({
      'le1.d1': ['2000', '4000.2'],
    });
  });

  it('recognizes antebytes and bytes records', () => {
    const ddb = new DynamodbData();

    expect(ddb.check({})).to.be.false;
    expect(ddb.check({ type: 'postbytes' })).to.be.false;

    expect(ddb.check({ type: 'antebytes' })).to.be.true;
    expect(ddb.check({ type: 'bytes' })).to.be.true;
    expect(ddb.check({ type: 'segmentbytes' })).to.be.true;
  });

  it('ignores duplicate bytes and segmentbytes records', () => {
    const isDuplicate = true;
    const cause = 'digestCache';
    const ddb = new DynamodbData([
      { type: 'antebytes', isDuplicate, cause, listenerEpisode: 'le1', digest: 'd1' },
      { type: 'bytes', isDuplicate, cause, listenerEpisode: 'le3', digest: 'd3' },
      { type: 'segmentbytes', isDuplicate, cause, listenerEpisode: 'le4', digest: 'd4' },
    ]);

    expect(Object.keys(ddb.payloads).length).to.equal(1);
    expect(Object.keys(ddb.segments).length).to.equal(0);
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
    expect(ddb.decodeSegment(`${epochMs}.0`)).to.eql(['20210617', epochMs, 0]);
    expect(ddb.decodeSegment(`${epochMs}.4`)).to.eql(['20210617', epochMs, 4]);
    expect(ddb.decodeSegment(`${epochMs}.whatev`)).to.eql(['20210617', epochMs, 'whatev']);
  });

  describe('#insert', () => {
    const led = { listenerEpisode: 'le1', digest: 'd1' };
    const download = { the: 'download' };
    const imp1 = { segment: 1, num: 1 };
    const imp3 = { segment: 3, num: 3 };
    const impressions = [imp1, imp3];
    const redirect = { type: 'antebytes', timestamp: 1000, download, impressions, ...led };
    const durations = [12.85924, 948.9482285, 1.5846666666];
    const types = 'aoi';
    const bytes1 = { type: 'segmentbytes', timestamp: 1001, segment: 3, ...led };
    const bytes2 = { type: 'bytes', timestamp: 1002, durations, types, ...led };
    const bytes3 = { type: 'bytes', timestamp: 100000, durations, types, ...led };

    it('logs kinesis impressions', async () => {
      sinon.stub(logger, 'info');
      sinon.stub(dynamo, 'updateItemPromise').callsFake(async () => ({}));

      const ddb = new DynamodbData([redirect, bytes1, bytes2, bytes3]);
      const result = await ddb.insert();
      expect(result).to.eql([
        { count: 1, dest: 'dynamodb' },
        { count: 2, dest: 'kinesis' },
      ]);

      expect(dynamo.updateItemPromise).to.have.callCount(1);
      expect(dynamo.updateItemPromise.args[0][0].Key).to.eql({ id: { S: 'le1.d1' } });

      expect(logger.info).to.have.callCount(2);
      expect(logger.info.args[0][0]).to.equal('impression');
      expect(logger.info.args[0][1]).to.eql({
        type: 'postbytes',
        timestamp: 1002,
        download,
        durations,
        types,
        impressions: [imp3],
        ...led,
      });
      expect(logger.info.args[1][0]).to.equal('impression');
      expect(logger.info.args[1][1]).to.eql({
        type: 'postbytes',
        timestamp: 100000,
        download,
        durations,
        types,
        impressions: [],
        ...led,
      });
    });

    it('overrides any msg stored on the redirect', async () => {
      sinon.stub(console, 'info');
      sinon.stub(dynamo, 'updateItemPromise').callsFake(async () => ({}));

      const ddb = new DynamodbData([{ ...redirect, msg: 'blah' }, bytes3]);
      const result = await ddb.insert();
      expect(result).to.eql([
        { count: 1, dest: 'dynamodb' },
        { count: 1, dest: 'kinesis' },
      ]);

      // look directly at console.info, to see the resolved lambda-log json
      expect(console.info).to.have.callCount(1);
      const obj = JSON.parse(console.info.args[0][0]);
      expect(obj['_logLevel']).to.equal('info');
      expect(obj['msg']).to.equal('impression');
      expect(obj['listenerEpisode']).to.equal('le1');
      expect(obj['digest']).to.equal('d1');
    });

    it('handles the redirect coming after the byte downloads', async () => {
      sinon.stub(logger, 'info');

      // pretend we're storing/returning DDB attributes
      let attrs = {};
      sinon.stub(dynamo, 'updateItemPromise').callsFake(async params => {
        const prev = { Attributes: { ...attrs } };
        const updates = params.AttributeUpdates;
        Object.keys(updates).forEach(k => (attrs[k] = updates[k].Value));
        return prev;
      });

      const ddb = new DynamodbData([bytes2, bytes3]);
      const result = await ddb.insert();
      expect(result).to.eql([{ count: 1, dest: 'dynamodb' }]);
      expect(dynamo.updateItemPromise).to.have.callCount(1);
      expect(logger.info).to.have.callCount(0);

      const ddb2 = new DynamodbData([redirect, bytes1]);
      const result2 = await ddb2.insert();
      expect(result2).to.eql([
        { count: 1, dest: 'dynamodb' },
        { count: 2, dest: 'kinesis' },
      ]);
      expect(dynamo.updateItemPromise).to.have.callCount(2);
      expect(logger.info).to.have.callCount(2);
    });

    it('throws an error on any ddb failures', async () => {
      sinon.stub(logger, 'error');
      sinon.stub(logger, 'warn');
      sinon.stub(logger, 'info');
      sinon.stub(dynamo, 'updateItemPromise').callsFake(async params => {
        if (params.Key.id.S.includes('.d2')) {
          throw new Error('terrible things');
        } else {
          return {};
        }
      });

      let err = null;
      try {
        const ddb = new DynamodbData([redirect, bytes1, { ...bytes2, digest: 'd2' }]);
        await ddb.insert();
      } catch (e) {
        err = e;
      }
      if (err) {
        expect(err.message).to.match(/DDB retrying/);
        expect(err.skipLogging).to.equal(true);
      } else {
        expect.fail('should have thrown an error');
      }

      expect(logger.error).to.have.callCount(1);
      expect(logger.error.args[0][0]).to.match(/terrible things/);

      expect(logger.warn).to.have.callCount(1);
      expect(logger.warn.args[0][0]).to.match(/DDB retrying/);

      expect(logger.info).to.have.callCount(1);
      expect(logger.info.args[0][1].timestamp).to.eql(bytes1.timestamp);

      expect(dynamo.updateItemPromise).to.have.callCount(2);
      expect(dynamo.updateItemPromise.args[0][0].Key).to.eql({ id: { S: 'le1.d2' } });
      expect(dynamo.updateItemPromise.args[1][0].Key).to.eql({ id: { S: 'le1.d1' } });
    });

    it('only warns on throughput exceeded errors', async () => {
      sinon.stub(logger, 'error');
      sinon.stub(logger, 'warn');
      sinon.stub(dynamo, 'updateItemPromise').callsFake(async params => {
        const err = new Error('terrible things');
        err.name = 'ProvisionedThroughputExceededException';
        throw err;
      });

      let err = null;
      try {
        const ddb = new DynamodbData([redirect, bytes1]);
        await ddb.insert();
      } catch (e) {
        err = e;
      }
      if (err) {
        expect(err.message).to.match(/DDB retrying/);
        expect(err.skipLogging).to.equal(true);
      } else {
        expect.fail('should have thrown an error');
      }

      expect(logger.error).to.have.callCount(0);

      expect(logger.warn).to.have.callCount(2);
      expect(logger.warn.args[0][0]).to.match(/DDB throughput exceeded/);
      expect(logger.warn.args[1][0]).to.match(/DDB retrying/);

      expect(dynamo.updateItemPromise).to.have.callCount(1);
      expect(dynamo.updateItemPromise.args[0][0].Key).to.eql({ id: { S: 'le1.d1' } });
    });
  });

  describe('updateAll', () => {
    it('limits concurrent dynamo updates', async () => {
      const ddb = new DynamodbData();
      sinon.stub(logger, 'error');
      sinon.stub(dynamo, 'client').callsFake(async () => 'my-client');
      sinon.stub(ddb, 'format').callsFake(() => []);

      // stub promises as update is called
      const promises = [],
        resolvers = [],
        rejectors = [];
      sinon.stub(dynamo, 'update').callsFake(() => {
        const p = new Promise((res, rej) => {
          resolvers.push(res);
          rejectors.push(rej);
        });
        promises.push(p);
        return p;
      });

      // to start, we should see 5 calls
      const args = ['my-id', { my: 'data' }, ['my', 'segments']];
      const updateAllPromise = ddb.updateAll(Array(10).fill(args), 5);
      await new Promise(r => process.nextTick(r));
      expect(promises.length).to.equal(5);

      // resolving any picks up new
      resolvers[0](0);
      resolvers[3](3);
      resolvers[4](4);
      await new Promise(r => process.nextTick(r));
      expect(promises.length).to.equal(8);

      // as do errors
      rejectors[1](1);
      rejectors[5](5);
      await new Promise(r => process.nextTick(r));
      expect(promises.length).to.equal(10);
      expect(logger.error).to.have.callCount(2);
      expect(logger.error.args[0][0]).to.match(/DDB Error/);
      expect(logger.error.args[1][0]).to.match(/DDB Error/);

      // finish up
      resolvers[2](2);
      resolvers[6](6);
      resolvers[7](7);
      resolvers[8](8);
      resolvers[9](9);

      const [updateCount, failureCount, loggedCount] = await updateAllPromise;
      expect(updateCount).to.equal(8);
      expect(failureCount).to.equal(2);
      expect(loggedCount).to.equal(0);
    });
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
      const records = ddb.format(['le.d', { download: 'something' }, segments]);

      expect(records.length).to.equal(2);
      expect(records[0]).to.eql({
        type: 'postbytes',
        listenerEpisode: 'le',
        digest: 'd',
        timestamp: 1000,
        download: 'something',
      });
      expect(records[1]).to.eql({
        type: 'postbytes',
        listenerEpisode: 'le',
        digest: 'd',
        timestamp: 100000,
        download: 'something',
      });
    });

    it('filters downloads', () => {
      const download = { the: 'download' };
      const impressions = [{ segment: 1 }, { segment: 2 }];
      const segments = {
        1000: true,
        100000: false,
        100000.1: true,
        200000: true,
        200000.2: true,
      };
      const records = ddb.format(['le.d', { download, impressions }, segments]);

      expect(records.length).to.equal(3);
      expect(records[0].download).to.eql(download);
      expect(records[1].download).to.be.undefined;
      expect(records[2].download).to.eql(download);
    });

    it('filters impressions', () => {
      const download = { the: 'download' };
      const impressions = [
        { segment: 0, num: 'imp0' },
        { segment: 3, num: 'imp3' },
        { segment: 2, num: 'imp2' },
      ];
      const segments = {
        1000: true,
        100000.2: true,
        '100001.0': false,
        '100002.0': true,
        '200000.0': true,
        200001.2: false,
        200002.3: true,
      };
      const records = ddb.format(['le.d', { download, impressions }, segments]);

      expect(records.length).to.equal(3);
      expect(records[0].timestamp).to.equal(1000);
      expect(records[0].impressions).to.eql([]);
      expect(records[1].timestamp).to.equal(100000);
      expect(records[1].impressions).to.eql([impressions[2]]);
      expect(records[2].timestamp).to.equal(200002);
      expect(records[2].impressions).to.eql([impressions[0], impressions[1]]);
    });

    it('removes records with no download and 0 impressions', () => {
      const download = { the: 'download' };
      const impressions = [{ segment: 1 }, { segment: 2 }];
      const segments = {
        1000: false,
        1000.9: true,
        100000.1: false,
        100000.9: true,
        200000.9: true,
      };
      const records = ddb.format(['le.d', { download, impressions }, segments]);
      expect(records.length).to.equal(0);
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
