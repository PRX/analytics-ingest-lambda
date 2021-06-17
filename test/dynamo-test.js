'use strict';

require('./support');
const dynamo = require('../lib/dynamo');
const logger = require('../lib/logger');
function throws(promise) {
  return promise.then(() => expect.fail('should have thrown an error'), err => err);
}

describe('dynamo', () => {

  describe('#updateAll', () => {

    it('limits concurrent updates', async () => {
      sinon.stub(logger, 'error');
      sinon.stub(dynamo, 'client').callsFake(async () => 'my-client');

      // stub promises as update is called
      const promises = [], resolvers = [], rejectors = [];
      sinon.stub(dynamo, 'update').callsFake(() => {
        const p = new Promise((res, rej) => {
          resolvers.push(res);
          rejectors.push(rej);
        });
        promises.push(p);
        return p;
      })

      // to start, we should see 5 calls
      const args = ['my-id', {my: 'data'}, ['my', 'segments']]
      const updateAllPromise = dynamo.updateAll(Array(10).fill(args), 5);
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
      expect(logger.error.args[0][0]).to.match(/DDB Error/)
      expect(logger.error.args[1][0]).to.match(/DDB Error/)

      // finish up
      resolvers[2](2);
      resolvers[6](6);
      resolvers[7](7);
      resolvers[8](8);
      resolvers[9](9);

      const result = await updateAllPromise;
      expect(result.success).to.eql([0, 3, 4, 2, 6, 7, 8, 9]);
      expect(result.failures).to.eql([args, args]);
    });

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
      expect(params.AttributeUpdates.expiration.Action).to.eql('PUT');
      if (params.AttributeUpdates.expiration.Value.N === `${now + 100}`) {
        expect(params.AttributeUpdates.expiration.Value).to.eql({N: `${now + 100}`});
      } else {
        expect(params.AttributeUpdates.expiration.Value).to.eql({N: `${now + 101}`});
      }
    });

  });

  describe('#updateResult', () => {

    it('returns null payload', async () => {
      const Attributes = {segments: {SS: ['1', '2']}};
      expect(await dynamo.updateResult('id', null, null, {})).to.eql(['id', null, null]);
      expect(await dynamo.updateResult('id', null, ['1'], {})).to.eql(['id', null, {1: false}])
      expect(await dynamo.updateResult('id', null, ['1'], {Attributes})).to.eql(['id', null, {1: false, 2: false}])
    });

    it('returns null when no segments', async () => {
      const Attributes = {payload: {B: await dynamo.deflate({foo: 'bar'})}};
      expect(await dynamo.updateResult('id', {foo: 'bar'}, null, {})).to.eql(['id', {foo: 'bar'}, null]);
      expect(await dynamo.updateResult('id', {foo: 'bar'}, [], {Attributes})).to.eql(['id', {foo: 'bar'}, null]);
    });

    it('returns null when segments all already set', async () => {
      const Attributes = {
        payload: {B: await dynamo.deflate({foo: 'bar'})},
        segments: {SS: ['1', '2', '3']}
      };

      const result1 = await dynamo.updateResult('id', {foo: 'bar'}, ['1'], {Attributes});
      expect(result1).to.eql(['id', {foo: 'bar'}, {1: false, 2: false, 3: false}]);

      const result2 = await dynamo.updateResult('id', {foo: 'bar'}, ['2', '1'], {Attributes});
      expect(result2).to.eql(result1);

      const result3 = await dynamo.updateResult('id', {foo: 'bar'}, ['2', '3', '1'], {Attributes});
      expect(result3).to.eql(result1);
    });

    it('returns all segments when first setting payload', async () => {
      const Attributes = {segments: {SS: ['1', '2']}};
      const result = await dynamo.updateResult('my-id', {foo: 'bar'}, ['2', '3'], {Attributes});
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({foo: 'bar'});
      expect(result[2]).to.eql({1: true, 2: true, 3: true});
    });

    it('returns all segments when first setting segments', async () => {
      const Attributes = {payload: {B: await dynamo.deflate({foo: 'bar'})}};
      const result = await dynamo.updateResult('my-id', null, ['1', '2'], {Attributes});
      expect(result.length).to.equal(3);
      expect(result[0]).to.equal('my-id');
      expect(result[1]).to.eql({foo: 'bar'});
      expect(result[2]).to.eql({1: true, 2: true});
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
      expect(result[2]).to.eql({1: false, 2: false, 3: true});
    });

  });

  // only run actual "update" tests with a real table
  (process.env.TEST_DDB_TABLE ? describe : xdescribe)('with real dynamodb', () => {
    const DATA = {hello: 'world', number: 10};

    beforeEach(async () => {
      process.env.DDB_TABLE = process.env.TEST_DDB_TABLE;
      process.env.DDB_ROLE = process.env.TEST_DDB_ROLE || '';
      await dynamo.delete('testid1');
    });

    describe('#update', () => {

      it('round trips payload data', async () => {
        expect(await dynamo.update('testid1', DATA)).to.eql(['testid1', DATA, null]);
        expect(await dynamo.update('testid1', null, ['1'])).to.eql(['testid1', DATA, {1: true}]);
      });

      it('sets an expiration', async () => {
        process.env.DDB_TTL = 100;
        expect(await dynamo.update('testid1', DATA)).to.eql(['testid1', DATA, null]);

        // directly get item to check for expiration
        const result = await dynamo.get('testid1');
        expect(result.Item.expiration.N).to.match(/[0-9]+/)
      });

      it('returns new segments', async () => {
        expect(await dynamo.update('testid1')).to.eql(['testid1', null, null]);
        expect(await dynamo.update('testid1', null, ['1'])).to.eql(['testid1', null, {1: false}]);

        const result1 = await dynamo.update('testid1', DATA, null);
        expect(result1).to.eql(['testid1', DATA, {1: true}]);

        const result2 = await dynamo.update('testid1', null, [1, 2]);
        expect(result2).to.eql(['testid1', DATA, {1: false, 2: true}]);

        const result3 = await dynamo.update('testid1', null, ['1', 2]);
        expect(result3).to.eql(['testid1', DATA, {1: false, 2: false}]);
      });

    });

    describe('#updateAll', () => {

      it('runs multiple updates', async () => {
        const updates = [
          ['testid1', null, ['one']],
          ['testid1', DATA, null],
          ['testid1', DATA, ['one', 'two']],
        ];

        const result = await dynamo.updateAll(updates);
        expect(result.success.length).to.equal(3);
        expect(result.failures.length).to.equal(0);
      });

    });

  });

});
