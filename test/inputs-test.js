'use strict';

const support = require('./support');
const Inputs  = require('../lib/inputs');

describe('inputs', () => {

  it('groups records by type', () => {
    let inputs = new Inputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0},
      {type: 'download',   requestUuid: 'd1', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 0},
      {type: 'impression', requestUuid: 'i3', timestamp: 0},
      {type: 'download',   requestUuid: 'd2', timestamp: 0}
    ]);
    expect(inputs.tables.length).to.equal(2);
    expect(inputs.tables).to.contain('the_downloads_table$19700101');
    expect(inputs.tables).to.contain('the_impressions_table$19700101');

    return inputs.formatRecords().then(recs => {
      let downs = recs.find(r => r[0] === 'the_downloads_table$19700101')[1];
      let imps = recs.find(r => r[0] === 'the_impressions_table$19700101')[1];
      expect(downs.length).to.equal(2);
      expect(downs[0].json.request_uuid).to.equal('d1');
      expect(downs[1].json.request_uuid).to.equal('d2');
      expect(imps.length).to.equal(3);
      expect(imps[0].json.request_uuid).to.equal('i1');
      expect(imps[1].json.request_uuid).to.equal('i2');
      expect(imps[2].json.request_uuid).to.equal('i3');
    });
  });

  it('groups records by date', () => {
    let inputs = new Inputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 1490831999999},
      {type: 'download',   requestUuid: 'd1', timestamp: 1490831999999},
      {type: 'impression', requestUuid: 'i2', timestamp: 1490832000000}
    ]);
    expect(inputs.tables.length).to.equal(3);
    expect(inputs.tables).to.contain('the_downloads_table$20170329');
    expect(inputs.tables).to.contain('the_impressions_table$20170329');
    expect(inputs.tables).to.contain('the_impressions_table$20170330');

    return inputs.formatRecords().then(recs => {
      let downs = recs.find(r => r[0] === 'the_downloads_table$20170329')[1];
      let imps1 = recs.find(r => r[0] === 'the_impressions_table$20170329')[1];
      let imps2 = recs.find(r => r[0] === 'the_impressions_table$20170330')[1];
      expect(downs.length).to.equal(1);
      expect(downs[0].json.request_uuid).to.equal('d1');
      expect(imps1.length).to.equal(1);
      expect(imps1[0].json.request_uuid).to.equal('i1');
      expect(imps2.length).to.equal(1);
      expect(imps2[0].json.request_uuid).to.equal('i2');
    });
  });

  it('handles unrecognized records', () => {
    let inputs = new Inputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0},
      {type: 'foobar',     requestUuid: 'fb', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 0},
      {what: 'ever'}
    ]);
    expect(inputs.tables.length).to.equal(1);
    expect(inputs.unrecognized.length).to.equal(2);
    expect(inputs.unrecognized[0].requestUuid).to.equal('fb');
    expect(inputs.unrecognized[1].what).to.equal('ever');
  });

});
