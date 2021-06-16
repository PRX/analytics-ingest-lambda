'use strict';

require('./support');
const dynamo = require('../lib/dynamo');
function throws(promise) {
  return promise.then(() => expect.fail('should have thrown an error'), err => err);
}

describe('dynamo', () => {

  describe('#update', () => {
  });

  describe('#updateParams', () => {
    it('requires a DDB_TABLE env', async () => {
      delete process.env.DDB_TABLE;
      expect(await throws(dynamo.update())).to.match(/must set a DDB_TABLE/i);
    });

    it('sets params', async () => {
      const params = await dynamo.updateParams('my-id');
      expect(params.Key).to.eql({id: {S: 'my-id'}});
      expect(params.ReturnValues).to.eql('ALL_OLD');
      expect(params.TableName).to.eql(process.env.DDB_TABLE);
    });

    it('deflates payloads', async () => {
      const params = await dynamo.updateParams('my-id', {some: 'payload'});
      expect(params.AttributeUpdates).to.have.keys('payload');
      expect(params.AttributeUpdates.payload.Action).to.eql('PUT');

      const deflated = await dynamo.deflate({some: 'payload'});
      expect(params.AttributeUpdates.payload.Value.B).to.eql(deflated);
    });

    it('stringifies segments', async () => {
      const params = await dynamo.updateParams('my-id', null, [1, '2', 'three']);
      expect(params.AttributeUpdates).to.have.keys('segments');
      expect(params.AttributeUpdates.segments.Action).to.eql('ADD');
      expect(params.AttributeUpdates.segments.Value).to.eql({SS: ['1', '2', 'three']});
    });

    it('optionally adds an expiration', async () => {
      process.env.DDB_TTL = 100;
      const now = Math.round(new Date().getTime() / 1000);

      const params = await dynamo.updateParams('my-id');
      expect(params.AttributeUpdates).to.have.keys('expiration');
      if (params.AttributeUpdates.expiration.N === `${now + 100}`) {
        expect(params.AttributeUpdates.expiration).to.eql({N: `${now + 100}`});
      } else {
        expect(params.AttributeUpdates.expiration).to.eql({N: `${now + 101}`});
      }
    });
  });

  describe('#updateResult', () => {
    it('returns null when no payload', async () => {
      const Attributes = {segments: {SS: ['1', '2']}};
      expect(await dynamo.updateResult('id', null, null, {})).to.be.null;
      expect(await dynamo.updateResult('id', null, ['1'], {})).to.be.null;
      expect(await dynamo.updateResult('id', null, ['1'], {Attributes})).to.be.null;
    });

    it('returns null when no segments', async () => {
      const Attributes = {payload: {B: await dynamo.deflate({foo: 'bar'})}};
      expect(await dynamo.updateResult('id', {foo: 'bar'}, null, {})).to.be.null;
      expect(await dynamo.updateResult('id', {foo: 'bar'}, [], {Attributes})).to.be.null;
    });

    it('returns null when segments all already set', async () => {
      const Attributes = {
        payload: {B: await dynamo.deflate({foo: 'bar'})},
        segments: {SS: ['1', '2', '3']}
      };
      expect(await dynamo.updateResult('id', {foo: 'bar'}, ['1'], {Attributes})).to.be.null;
      expect(await dynamo.updateResult('id', {foo: 'bar'}, ['2', '1'], {Attributes})).to.be.null;
      expect(await dynamo.updateResult('id', {foo: 'bar'}, ['2', '3', '1'], {Attributes})).to.be.null;
    });

    it('returns all segments when first setting payload', async () => {
      const Attributes = {segments: {SS: ['1', '2']}};
      const result = await dynamo.updateResult('my-id', {foo: 'bar'}, ['2', '3'], {Attributes});
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({foo: 'bar'});
      expect(result[2]).to.eql(['1', '2', '3']);
    });

    it('returns all segments when first setting segments', async () => {
      const Attributes = {payload: {B: await dynamo.deflate({foo: 'bar'})}};
      const result = await dynamo.updateResult('my-id', null, ['1', '2'], {Attributes});
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({foo: 'bar'});
      expect(result[2]).to.eql(['1', '2']);
    });

    it('returns new segments when subsequently setting segments', async () => {
      const Attributes = {
        payload: {B: await dynamo.deflate({foo: 'bar'})},
        segments: {SS: ['1', '2']}
      };
      const result = await dynamo.updateResult('my-id', {foo: 'changed'}, ['1', '2', '3'], {Attributes});
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({foo: 'changed'});
      expect(result[2]).to.eql(['3']);
    });
  });
});
