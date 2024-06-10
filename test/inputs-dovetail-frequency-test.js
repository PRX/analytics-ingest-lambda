'use strict';

require('./support');
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
    let inserts = {};
    let frequency2 = new DovetailFrequency([
      { type: 'impression', requestUuid: 'the-uuid1', timestamp: 1490827132999 },
      { type: 'download', requestUuid: 'the-uuid2', timestamp: 1490827132999 },
      { 
        type: 'postbytes',
        listenerId: 'listener1',
        timestamp: 1717978337,
        impressions: []
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: 1717978361,
        impressions: [{ campaignId: 100 }, { campaignId: 200 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener3',
        timestamp: 1717978391,
        impressions: [{ isDuplicate: true, campaignId: 300 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: 1715395374,
        impressions: [{ campaignId: 400 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: 1717978419,
        impressions: [{ campaignId: 500 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: 1717978432,
        impressions: [{ campaignId: 500 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener4',
        timestamp: 1717978432,
        impressions: [{ campaignId: 500 }],
      },
      {
        type: 'postbytes',
        listenerId: 'listener2',
        timestamp: 1717978400,
        impressions: [{ campaignId: 400 }],
      },
    ]);
    return frequency2.insert().then(result => {
      expect(result).to.equal(7);
    });
  });
});
