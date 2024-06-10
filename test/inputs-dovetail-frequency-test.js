'use strict';

require('./support');
const AWS = require('aws-sdk');
const dynamo = require('../lib/dynamo');
const DovetailFrequency = require('../lib/inputs/dovetail-frequency');

describe('dovetail-frequency', () => {
  let impression = new DovetailFrequency();

  it('recognizes impression records', () => {
    expect(impression.check({ type: null })).to.be.false;
    expect(impression.check({ type: undefined })).to.be.false;
    expect(impression.check({ type: 'download' })).to.be.false;
    expect(impression.check({ type: 'impression' })).to.be.false;
    expect(impression.check({ type: 'postbytes', impressions: [] })).to.be.false;
    expect(impression.check({ type: 'postbytes', impressions: [{}] })).to.be.true;
  });

  it('knows the table names of records', () => {
    expect(impression.tableName()).to.equal('DovetailListenerFrequency');
  });

  it('inserts nothing', () => {
    return impression.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts impression records', () => {
    if (process.env.DDB_LOCAL) {
      const localClient = new AWS.DynamoDB({
        apiVersion: "2012-08-10",
        accessKeyId: "None",
        secretAccessKey: "None",
        region: "local",
        endpoint: `http://${process.env.DDB_LOCAL}:8000`
      });
      sinon.stub(dynamo, 'client').callsFake(async () => localClient);
    } else {
      sinon.stub(dynamo, 'client').callsFake(async () => 'my-client');
      sinon.stub(dynamo, 'updateItemPromise').callsFake(async params => {
        let exp = '';
        let imp = [];
        if (params.ExpressionAttributeValues[":ttl"]) {
          exp = params.ExpressionAttributeValues[":ttl"].N;
          imp = params.ExpressionAttributeValues[":ts"].NS;
        }
        return {
          Attributes: {
            listener: { S: params.Key.listener.S },
            campaign: { S: params.Key.campaign.S },
            expiration: { N: exp },
            impressions: { NS: imp }
          }
        }
      });
    }

    let inserts = {};
    const current = Math.floor((new Date().getTime() / 1000));
    let frequency = new DovetailFrequency([
      { type: 'impression', requestUuid: 'the-uuid1', timestamp: 1490827132999 },
      { type: 'download', requestUuid: 'the-uuid2', timestamp: 1490827132999 },
      { 
        type: 'postbytes',
        listenerId: 'listener1',
        timestamp: `${current - 100}`,
        impressions: []
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: `${current - 90}`,
        impressions: [{ campaignId: 100 }, { campaignId: 200 }],
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
        impressions: [{ campaignId: 400 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: `${current - 60}`,
        impressions: [{ campaignId: 500 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: `${current - 50}`,
        impressions: [{ campaignId: 500 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: `${current - 50}`,
        impressions: [{ campaignId: 500 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: `${current - 40}`,
        impressions: [{ campaignId: 400 }],
      },
    ]);
    return frequency.insert().then(result => {
      expect(result).to.eql([
        { count: 7, dest: 'updates' },
        { count: 1, dest: 'deletes' },
      ]);
    });
  });
});
