'use strict';
require('dotenv').config();

require('./support');

const dynamo = require('../lib/dynamo');
const logger = require('../lib/logger');
function throws(promise) {
  return promise.then(
    () => expect.fail('should have thrown an error'),
    err => err,
  );
}

describe('dynamo', () => {
  describe('#updateParams', () => {
    it('requires a DDB_TABLE env', async () => {
      delete process.env.DDB_TABLE;
      expect(await throws(dynamo.update())).to.match(/must set a DDB_TABLE/i);
    });

    it('sets params', async () => {
      const params = await dynamo.updateParams('my-id');
      expect(params.Key).to.eql({ id: { S: 'my-id' } });
      expect(params.ReturnValues).to.eql('ALL_OLD');
      expect(params.TableName).to.eql(process.env.DDB_TABLE);
    });

    it('deflates payloads', async () => {
      const params = await dynamo.updateParams('my-id', { some: 'payload' });
      expect(params.AttributeUpdates).to.have.keys('payload');
      expect(params.AttributeUpdates.payload.Action).to.eql('PUT');

      const deflated = await dynamo.deflate({ some: 'payload' });
      expect(params.AttributeUpdates.payload.Value.B).to.eql(deflated);
    });

    it('stringifies segments', async () => {
      const params = await dynamo.updateParams('my-id', null, [1, '2', 'three']);
      expect(params.AttributeUpdates).to.have.keys('segments');
      expect(params.AttributeUpdates.segments.Action).to.eql('ADD');
      expect(params.AttributeUpdates.segments.Value).to.eql({ SS: ['1', '2', 'three'] });
    });

    it('optionally adds an expiration', async () => {
      process.env.DDB_TTL = 100;
      const now = Math.round(new Date().getTime() / 1000);

      const params = await dynamo.updateParams('my-id');
      expect(params.AttributeUpdates).to.have.keys('expiration');
      expect(params.AttributeUpdates.expiration.Action).to.eql('PUT');
      if (params.AttributeUpdates.expiration.Value.N === `${now + 100}`) {
        expect(params.AttributeUpdates.expiration.Value).to.eql({ N: `${now + 100}` });
      } else {
        expect(params.AttributeUpdates.expiration.Value).to.eql({ N: `${now + 101}` });
      }
    });
  });

  describe('#updateResult', () => {
    it('returns null payload', async () => {
      const Attributes = { segments: { SS: ['1', '2'] } };
      expect(await dynamo.updateResult('id', null, null, null, {})).to.eql(['id', null, null]);
      expect(await dynamo.updateResult('id', null, ['1'], null, {})).to.eql([
        'id',
        null,
        { 1: false },
      ]);
      expect(await dynamo.updateResult('id', null, ['1'], null, { Attributes })).to.eql([
        'id',
        null,
        { 1: false, 2: false },
      ]);
    });

    it('returns null when no segments', async () => {
      const Attributes = { payload: { B: await dynamo.deflate({ foo: 'bar' }) } };
      expect(await dynamo.updateResult('id', { foo: 'bar' }, null, null, {})).to.eql([
        'id',
        { foo: 'bar' },
        null,
      ]);
      expect(await dynamo.updateResult('id', { foo: 'bar' }, [], null, { Attributes })).to.eql([
        'id',
        { foo: 'bar' },
        null,
      ]);
    });

    it('returns null when segments all already set', async () => {
      const Attributes = {
        payload: { B: await dynamo.deflate({ foo: 'bar' }) },
        segments: { SS: ['1', '2', '3'] },
      };

      const result1 = await dynamo.updateResult('id', { foo: 'bar' }, ['1'], null, { Attributes });
      expect(result1).to.eql(['id', { foo: 'bar' }, { 1: false, 2: false, 3: false }]);

      const result2 = await dynamo.updateResult('id', { foo: 'bar' }, ['2', '1'], null, {
        Attributes,
      });
      expect(result2).to.eql(result1);

      const result3 = await dynamo.updateResult('id', { foo: 'bar' }, ['2', '3', '1'], null, {
        Attributes,
      });
      expect(result3).to.eql(result1);
    });

    it('returns all segments when first setting payload', async () => {
      const Attributes = { segments: { SS: ['1', '2'] } };
      const result = await dynamo.updateResult('my-id', { foo: 'bar' }, ['2', '3'], null, {
        Attributes,
      });
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({ foo: 'bar' });
      expect(result[2]).to.eql({ 1: true, 2: true, 3: true });
    });

    it('returns all segments when first setting segments', async () => {
      const Attributes = { payload: { B: await dynamo.deflate({ foo: 'bar' }) } };
      const result = await dynamo.updateResult('my-id', null, ['1', '2'], null, { Attributes });
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({ foo: 'bar' });
      expect(result[2]).to.eql({ 1: true, 2: true });
    });

    it('returns new segments when subsequently setting segments', async () => {
      const Attributes = {
        payload: { B: await dynamo.deflate({ foo: 'bar' }) },
        segments: { SS: ['1', '2'] },
      };
      const result = await dynamo.updateResult('my-id', { foo: 'changed' }, ['1', '2', '3'], null, {
        Attributes,
      });
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({ foo: 'changed' });
      expect(result[2]).to.eql({ 1: false, 2: false, 3: true });
    });

    it('merges just-set extras into the payload', async () => {
      const payload = { foo: 'bar' };
      const extras = { extra: 'stuff' };
      const result = await dynamo.updateResult('my-id', payload, [], extras, {});
      expect(result[1]).to.eql({ ...payload, ...extras });
    });

    it('merges previous extras into the payload', async () => {
      const payload = { foo: 'bar' };
      const extras = { extra: 'stuff' };
      const Attributes = {
        payload: { B: await dynamo.deflate(payload) },
        extras: { S: JSON.stringify(extras) },
      };
      const result = await dynamo.updateResult('my-id', null, null, null, { Attributes });
      expect(result[1]).to.eql({ ...payload, ...extras });
    });
  });

  // only run actual "update" tests with a real table
  (process.env.TEST_DDB_TABLE ? describe : xdescribe)('with real dynamodb', () => {
    const DATA = { hello: 'world', number: 10 };

    beforeEach(async () => {
      process.env.DDB_TABLE = process.env.TEST_DDB_TABLE;
      process.env.DDB_ROLE = process.env.TEST_DDB_ROLE || '';
      await dynamo.delete('testid1');
    });

    describe('#update', () => {
      it('round trips payload data', async () => {
        expect(await dynamo.update('testid1', DATA)).to.eql(['testid1', DATA, null]);
        expect(await dynamo.update('testid1', null, ['1'])).to.eql(['testid1', DATA, { 1: true }]);
      });

      it('sets an expiration', async () => {
        process.env.DDB_TTL = 100;
        expect(await dynamo.update('testid1', DATA)).to.eql(['testid1', DATA, null]);

        // directly get item to check for expiration
        const result = await dynamo.get('testid1');
        expect(result.Item.expiration.N).to.match(/[0-9]+/);
      });

      it('returns new segments', async () => {
        expect(await dynamo.update('testid1')).to.eql(['testid1', null, null]);
        expect(await dynamo.update('testid1', null, ['1'])).to.eql(['testid1', null, { 1: false }]);

        const result1 = await dynamo.update('testid1', DATA, null);
        expect(result1).to.eql(['testid1', DATA, { 1: true }]);

        const result2 = await dynamo.update('testid1', null, [1, 2]);
        expect(result2).to.eql(['testid1', DATA, { 1: false, 2: true }]);

        const result3 = await dynamo.update('testid1', null, ['1', 2]);
        expect(result3).to.eql(['testid1', DATA, { 1: false, 2: false }]);
      });

      it('merges extras into the payload', async () => {
        await dynamo.update('testid1', null, ['1'], { extra: 'stuff' });
        const result = await dynamo.update('testid1', DATA);
        expect(result[1]).to.eql({ ...DATA, extra: 'stuff' });
      });
    });
  });
});
