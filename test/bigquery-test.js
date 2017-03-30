'use strict';

const support  = require('./support');
const decrypt  = require('../lib/decrypt');
const bigquery = require('../lib/bigquery');

describe('bigquery', () => {

  it('memoizes the dataset', () => {
    return bigquery.dataset(true).then(dataset => {
      return bigquery.dataset(false).then(dataset2 => {
        expect(dataset.id).to.equal(dataset2.id);
        expect(dataset).to.equal(dataset2);
      });
    });
  });

  it('detects encrypted looking private keys', () => {
    sinon.stub(decrypt, 'decryptAws', () => Promise.resolve('okay'));
    return bigquery.dataset(true).then(dataset => {
      expect(decrypt.decryptAws).not.to.have.been.called;
      process.env.BQ_PRIVATE_KEY = 'this-looks-encrypted';
      return bigquery.dataset(true).then(dataset2 => {
        expect(decrypt.decryptAws).to.have.been.called;
      });
    });
  });

  it('short circuits when inserting nothing', () => {
    return bigquery.insert('thetable', []).then(count => {
      expect(count).to.equal(0);
    });
  });

  describe('with a mocked dataset', () => {

    let inserted, insertOpts;
    beforeEach(() => {
      inserted = {};
      sinon.stub(bigquery, 'dataset', () => {
        return Promise.resolve({table: tbl => {
          return {insert: (rows, opts) => {
            inserted[tbl] = rows;
            insertOpts = opts;
            return Promise.resolve({});
          }};
        }});
      });
    });

    it('inserts raw rows', () => {
      return bigquery.insert('thetable', [{id: 'foo'}, {id: 'bar'}]).then(count => {
        expect(count).to.equal(2);
        expect(inserted).to.have.keys('thetable');
        expect(inserted.thetable.length).to.equal(2);
        expect(inserted.thetable[0].id).to.equal('foo');
        expect(inserted.thetable[1].id).to.equal('bar');
        expect(insertOpts.raw).to.equal(true);
      });
    });

  });

});
