'use strict';

const support = require('./support');
const pingurl = require('../lib/bigquery');
const AdzerkImpressions = require('../lib/inputs/adzerk-impressions');

describe('adzerk-impressions', () => {

  let adzerk = new AdzerkImpressions();

  it('recognizes impression url records', () => {
    expect(adzerk.check({})).to.be.false;
    expect(adzerk.check({impressionUrl: null})).to.be.false;
    expect(adzerk.check({impressionUrl: false})).to.be.false;
    expect(adzerk.check({impressionUrl: ''})).to.be.false;
    expect(adzerk.check({impressionUrl: 'foo'})).to.be.true;
    expect(adzerk.check({impressionUrl: 'foo', isDuplicate: true})).to.be.false;
    expect(adzerk.check({impressionUrl: 'foo', isDuplicate: false})).to.be.true;
  });

  it('inserts nothing', () => {
    return adzerk.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('pings impression url records', () => {
    let ping1 = nock('http://www.foo.bar').get('/ping1').reply(200);
    let ping2 = nock('https://www.foo.bar').get('/ping2').reply(200);
    let ping3 = nock('http://bar.foo').get('/ping3').reply(200);
    let ping4 = nock('https://www.foo.bar').get('/ping4').reply(200);
    let ping5 = nock('http://www.foo.bar').get('/ping5').reply(200);

    let adzerk2 = new AdzerkImpressions([
      {isDuplicate: false, impressionUrl: 'http://www.foo.bar/ping1'},
      {isDuplicate: true,  impressionUrl: 'https://www.foo.bar/ping2'},
      {isDuplicate: false, impressionUrl: 'http://bar.foo/ping3'},
      {isDuplicate: false, impressionUrl: 'https://www.foo.bar/ping4'},
      {isDuplicate: false, impressionUrl: 'http://www.foo.bar/ping5'}
    ]);
    return adzerk2.insert().then(result => {
      expect(result.length).to.equal(2);
      expect(result[0].dest).to.equal('bar.foo');
      expect(result[0].count).to.equal(1);
      expect(result[1].dest).to.equal('www.foo.bar');
      expect(result[1].count).to.equal(3);
    });
  });

});
