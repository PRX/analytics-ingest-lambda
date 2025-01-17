'use strict';

require('./support');
const AWS = require('aws-sdk');
const dynamo = require('../lib/dynamo');
const DovetailFrequency = require('../lib/inputs/dovetail-frequency');

describe('dovetail-frequency', () => {
  let frequency = new DovetailFrequency();

  it('recognizes a frequency impression', () => {
    expect(frequency.checkFrequency({})).to.be.false;
    expect(frequency.checkFrequency({ frequency: null })).to.be.false;
    expect(frequency.checkFrequency({ frequency: '' })).to.be.false;
    expect(frequency.checkFrequency({ frequency: 'foo' })).to.be.false;
    expect(frequency.checkFrequency({ frequency: '1:1' })).to.be.true;
    expect(frequency.checkFrequency({ frequency: '1:1,2:4' })).to.be.true;
  });

  it('recognizes frequency impressions', () => {
    expect(
      frequency.checkImpressions([
        {},
        { frequency: null },
        { frequency: '' },
        { frequency: 'foo' },
      ]),
    ).to.be.false;
    expect(frequency.checkImpressions([{}, { frequency: '1:1' }, {}])).to.be.true;
    expect(frequency.checkImpressions([{}, { frequency: '1:1,2:4' }])).to.be.true;
  });

  it('recognizes impression records', () => {
    expect(frequency.check({ type: null })).to.be.false;
    expect(frequency.check({ type: undefined })).to.be.false;
    expect(frequency.check({ type: 'download' })).to.be.false;
    expect(frequency.check({ type: 'impression' })).to.be.false;
    expect(frequency.check({ type: 'postbytes', impressions: [] })).to.be.false;
    expect(frequency.check({ type: 'postbytes', impressions: [{}] })).to.be.false;
    expect(frequency.check({ type: 'postbytes', impressions: [{ frequency: '1:1' }] })).to.be.true;
  });

  it('knows the table names of records', () => {
    expect(frequency.tableName()).to.equal('listener_frequency_dev_local');
  });

  it('inserts nothing', () => {
    return frequency.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts impression records', () => {
    if (process.env.DDB_LOCAL) {
      const localClient = new AWS.DynamoDB({
        apiVersion: '2012-08-10',
        accessKeyId: 'None',
        secretAccessKey: 'None',
        region: 'local',
        endpoint: `http://${process.env.DDB_LOCAL}:8000`,
      });
      sinon.stub(dynamo, 'client').callsFake(async () => localClient);
    } else {
      sinon.stub(dynamo, 'client').callsFake(async () => 'my-client');
      sinon.stub(dynamo, 'updateItemPromise').callsFake(async params => {
        const current = Math.floor(new Date().getTime() / 1000);
        let exp = '';
        let imp = [
          `${current - 2600000}`,
          `${current - 2700000}`,
          `${current - 2800000}`,
          `${current - 2900000}`,
          `${current - 3000000}`,
          `${current - 3100000}`,
          `${current - 3200000}`,
          `${current - 3300000}`,
          `${current - 3400000}`,
          `${current - 3500000}`,
          `${current - 3600000}`,
        ];
        if (params.ExpressionAttributeValues[':ttl']) {
          exp = params.ExpressionAttributeValues[':ttl'].N;
          imp = imp.concat(params.ExpressionAttributeValues[':ts'].NS);
        }
        return {
          Attributes: {
            listener: { S: params.Key.listener.S },
            campaign: { S: params.Key.campaign.S },
            expiration: { N: exp },
            impressions: { NS: imp },
          },
        };
      });
    }

    let inserts = {};
    const current = Math.floor(new Date().getTime() / 1000);
    let frequency = new DovetailFrequency([
      { type: 'impression', requestUuid: 'the-uuid1', timestamp: 1490827132999 },
      { type: 'download', requestUuid: 'the-uuid2', timestamp: 1490827132999 },
      {
        type: 'postbytes',
        listenerId: 'listener1',
        timestamp: `${current - 100}`,
        impressions: [],
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: `${current - 90}`,
        impressions: [
          { campaignId: 100, frequency: '1:1' },
          { campaignId: 200, frequency: '1:1' },
        ],
      },
      {
        type: 'postbytes',
        listenerId: 'listener3',
        timestamp: `${current - 80}`,
        impressions: [{ isDuplicate: true, campaignId: 300 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: `${current - 2600000}`,
        impressions: [{ campaignId: 400, frequency: '1:1' }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: `${current - 60}`,
        impressions: [{ campaignId: 500, frequency: '1:1' }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: `${current - 50}`,
        impressions: [{ campaignId: 500, frequency: '1:1' }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: `${current - 50}`,
        impressions: [{ campaignId: 500, frequency: '1:1' }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: `${current - 40}`,
        impressions: [{ campaignId: 400, frequency: '1:1' }, { campaignId: 404 }],
      },
    ]);
    return frequency.insert().then(result => {
      expect(result).to.eql([
        { count: 6, dest: 'updates' },
        { count: 6, dest: 'deletes' },
      ]);
    });
  });
});
