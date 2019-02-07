'use strict';

require('./support');
const dynamo = require('../lib/dynamo');
const skipit = process.env.TEST_DDB_TABLE ? it : xit;

describe('dynamo', () => {

  beforeEach(() => {
    if (process.env.TEST_DDB_TABLE) {
      process.env.DDB_TABLE = process.env.TEST_DDB_TABLE;
      return dynamo.delete(['testid1', 'testid2']);
    }
  });

  it('requires a DDB_TABLE env', async () => {
    let err = null;
    try {
      delete process.env.DDB_TABLE;
      await dynamo.get('anything');
    } catch (e) {
      err = e;
    }
    if (err) {
      expect(err.message).to.match(/must set a DDB_TABLE/);
    } else {
      expect.fail('should have thrown an error');
    }
  });

  it('requires an id field', async () => {
    let err = null;
    try {
      await dynamo.get('anything', null);
    } catch (e) {
      err = e;
    }
    if (err) {
      expect(err.message).to.match(/must specify an id field/);
    } else {
      expect.fail('should have thrown an error');
    }
  });

  it('requires identifiers to write', async () => {
    const data1 = [
      {id: 'testid1', hello: 'world', number: 10},
      {some: 'other1', things: 99},
      {id: 'testid2', some: 'other2', things: 100},
    ];
    let err = null;
    try {
      await dynamo.write(data1);
    } catch (e) {
      err = e;
    }
    if (err) {
      expect(err.message).to.match(/must include the field 'id'/);
    } else {
      expect.fail('should have thrown an error');
    }
  });

  skipit('round trips data', async () => {
    const data1 = {id: 'testid1', hello: 'world', number: 10};
    expect(await dynamo.write(data1)).to.equal(1);

    const data2 = await dynamo.get('testid1');
    expect(data2).to.eql(data1);
  });

  skipit('deletes data', async () => {
    await dynamo.write([{id: 'testid1'}, {id: 'testid2'}]);
    const ids = ['testid2', 'testid1', 'testid1', 'this-does-not-exist'];
    expect(await dynamo.delete(ids)).to.equal(3);
  });

  skipit('gets null for missing keys', async () => {
    expect(await dynamo.get('this-does-not-exist')).to.be.null;
  });

  skipit('overrides the id field', async () => {
    const data1 = [
      {mykey: 'testid1', hello: 'world', number: 10},
      {mykey: 'testid2', some: 'other', things: 99},
    ];
    expect(await dynamo.write(data1, 'mykey')).to.equal(2);

    const data2 = await dynamo.get(['testid1', 'testid2'], 'mykey');
    expect(data2.length).to.equal(2);
    expect(data2).to.eql(data1);
  });

  skipit('returns data in the order of the keys', async () => {
    const data1 = [
      {id: 'testid1', hello: 'world', number: 10},
      {id: 'testid2', some: 'other1', things: 99},
      {id: 'testid2', some: 'other2', things: 100},
    ];
    expect(await dynamo.write(data1)).to.equal(2);

    const data2 = await dynamo.get(['testid2', 'testid1', 'this-does-not-exist', 'testid1']);
    expect(data2.length).to.equal(4);
    expect(data2[0]).to.eql(data1[2]);
    expect(data2[3]).to.eql(data1[0]);
    expect(data2[2]).to.be.null;
    expect(data2[3]).to.eql(data1[0]);
  });

});
