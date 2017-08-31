'use strict';

const support = require('./support');
const Inputs  = require('../lib/inputs');
const bigquery = require('../lib/bigquery');
const logger = require('../lib/logger');

describe('inputs', () => {

  it('handles unrecognized records', () => {
    let inputs = new Inputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0},
      {type: 'foobar',     requestUuid: 'fb', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 0},
      {what: 'ever'}
    ]);
    expect(inputs.unrecognized.length).to.equal(2);
    expect(inputs.unrecognized[0].requestUuid).to.equal('fb');
    expect(inputs.unrecognized[1].what).to.equal('ever');
  });

  it('inserts all bigquery inputs', () => {
    sinon.stub(logger, 'info');
    sinon.stub(bigquery, 'insert', (tbl, rows) => Promise.resolve(rows.length));
    let inputs = new Inputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0, impressionUrl: 'http://foo.bar'},
      {type: 'foobar',     requestUuid: 'fb', timestamp: 0},
      {type: 'download',   requestUuid: 'd1', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 999999}
    ]);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(3);
      expect(inserts.map(i => i.count)).to.eql([1, 1, 1]);
      expect(inserts.map(i => i.dest).sort()).to.eql([
        'the_downloads_table$19700101',
        'the_impressions_table$19700101',
        'the_impressions_table$19700112'
      ]);
    });
  });

  it('inserts pingback inputs', () => {
    sinon.stub(logger, 'info');
    nock('http://foo.bar').get('/').reply(200);
    let inputs = new Inputs([
      {type: 'impression', requestUuid: 'i1', timestamp: 0, impressionUrl: 'http://foo.bar'},
      {type: 'foobar',     requestUuid: 'fb', timestamp: 0},
      {type: 'download',   requestUuid: 'd1', timestamp: 0},
      {type: 'impression', requestUuid: 'i2', timestamp: 999999}
    ], true);
    return inputs.insertAll().then(inserts => {
      expect(inserts.length).to.equal(1);
      expect(inserts[0].count).to.equal(1);
      expect(inserts[0].dest).to.equal('foo.bar');
    });
  });

});
