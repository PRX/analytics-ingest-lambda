'use strict';

require('./support');
const kinesis = require('../lib/kinesis');

describe('kinesis', () => {

  let puts = [];
  beforeEach(() => {
    puts = [];
    sinon.stub(kinesis.client, 'putRecords').callsFake(({Records}) => {
      puts.push(Records);
      return Promise.resolve({});
    });
  });

  it('requires a KINESIS_STREAM env', async () => {
    let err = null;
    try {
      delete process.env.KINESIS_STREAM;
      await kinesis.put('anything');
    } catch (e) {
      err = e;
    }
    if (err) {
      expect(err.message).to.match(/must set a KINESIS_STREAM/);
    } else {
      expect.fail('should have thrown an error');
    }
  });

  it('puts nothing', async () => {
    expect(await kinesis.put([])).to.equal(0);
    expect(puts.length).to.equal(0);
  });

  it('puts records in chunks', async () => {
    const recs = Array(402).fill(null).map((n, i) => { return {idx: i}; });
    expect(await kinesis.put(recs)).to.equal(402);
    expect(puts.length).to.equal(3);
    expect(puts[0].length).to.equal(200);
    expect(puts[0][0].Data).to.equal('{"idx":0}');
    expect(puts[1].length).to.equal(200);
    expect(puts[1][0].Data).to.equal('{"idx":200}');
    expect(puts[2].length).to.equal(2);
    expect(puts[2][0].Data).to.equal('{"idx":400}');
  });

  it('uses the request uuid as a partition key', async () => {
    const recs = [{requestUuid: 'one'}, {requestUuid: 'two'}, {requestUuid: 'three'}];
    expect(await kinesis.put(recs)).to.equal(3);
    expect(puts[0][0].PartitionKey).to.equal('one');
    expect(puts[0][1].PartitionKey).to.equal('two');
    expect(puts[0][2].PartitionKey).to.equal('three');
  });

  it('uses the listener session as a partition key', async () => {
    const recs = [{listenerSession: 'one'}, {listenerSession: 'two'}, {listenerSession: 'three'}];
    expect(await kinesis.put(recs)).to.equal(3);
    expect(puts[0][0].PartitionKey).to.equal('one');
    expect(puts[0][1].PartitionKey).to.equal('two');
    expect(puts[0][2].PartitionKey).to.equal('three');
  });

  it('hashes the json as a partition key', async () => {
    const recs = [{thing: 'one'}, {thing: 'two'}, {thing: 'one'}, {thing: 'one', a: 1}];
    expect(await kinesis.put(recs)).to.equal(4);
    expect(puts[0][0].PartitionKey.length).to.be.above(20);
    expect(puts[0][0].PartitionKey.length).to.be.below(255);
    expect(puts[0][0].PartitionKey).not.to.equal(puts[0][1].PartitionKey);
    expect(puts[0][0].PartitionKey).to.equal(puts[0][2].PartitionKey);
    expect(puts[0][0].PartitionKey).not.to.equal(puts[0][3].PartitionKey);
  });

});
