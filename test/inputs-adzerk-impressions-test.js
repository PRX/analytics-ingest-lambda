'use strict';

const support = require('./support');
const logger = require('../lib/logger');
const AdzerkImpressions = require('../lib/inputs/adzerk-impressions');

describe('adzerk-impressions', () => {

  let adzerk = new AdzerkImpressions();

  it('recognizes impression url records', () => {
    expect(adzerk.check({})).to.be.false;
    expect(adzerk.check({impressionUrl: 'foo', isDuplicate: false})).to.be.false;
    expect(adzerk.check({type: 'combined', impressions: []})).to.be.false;
    expect(adzerk.check({type: 'combined', impressions: [{impressionUrl: 'foo', isDuplicate: true}]})).to.be.false;
    expect(adzerk.check({type: 'combined', impressions: [{}, {impressionUrl: 'foo'}]})).to.be.true;
  });

  it('inserts nothing', () => {
    return adzerk.insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('pings impression url records', async () => {
    let ping1 = nock('http://www.foo.bar').get('/ping1').reply(200);
    let ping3 = nock('http://bar.foo').get('/ping3').reply(200);
    let ping4 = nock('https://www.foo.bar').get('/ping4').reply(200);
    let ping5 = nock('http://www.foo.bar').get('/ping5').reply(200);

    let adzerk2 = new AdzerkImpressions([
      {type: 'combined', impressions: [
        {impressionUrl: 'http://www.foo.bar/ping1'}
      ]},
      {isDuplicate: false, impressionUrl: 'http://www.foo.bar/nothing'},
      {type: 'combined', impressions: [
        {impressionUrl: 'https://www.foo.bar/ping2', isDuplicate: true},
        {impressionUrl: 'http://bar.foo/ping3'}
      ]},
      {type: 'combined', impressions: [
        {},
        {isDuplicate: false, impressionUrl: 'https://www.foo.bar/ping4'},
        {isDuplicate: false, impressionUrl: 'http://www.foo.bar/ping5'}
      ]}
    ]);
    return adzerk2.insert().then(result => {
      expect(result.length).to.equal(2);
      expect(result.map(r => r.dest).sort()).to.eql(['bar.foo', 'www.foo.bar']);
      expect(result.find(r => r.dest === 'bar.foo').count).to.equal(1);
      expect(result.find(r => r.dest === 'www.foo.bar').count).to.equal(3);
    });
  });

  it('complains about failed pings', () => {
    let ping1 = nock('http://foo.bar').get('/ping1').reply(404);
    let ping2a = nock('http://foo.bar').get('/ping2').times(2).reply(502);
    let ping2b = nock('http://foo.bar').get('/ping2').reply(200);
    let ping3 = nock('http://bar.foo').get('/ping3').times(3).reply(502);
    let adzerk3 = new AdzerkImpressions([
      {type: 'combined', impressions: [{isDuplicate: false, impressionUrl: 'http://foo.bar/ping1'}]},
      {type: 'combined', impressions: [
        {isDuplicate: false, impressionUrl: 'http://foo.bar/ping2'},
        {isDuplicate: false, impressionUrl: 'http://bar.foo/ping3'}
      ]}
    ], 1000, 0);

    let warns = [];
    sinon.stub(logger, 'warn', msg => warns.push(msg));

    return adzerk3.insert().then(result => {
      expect(result.length).to.equal(1);
      expect(result[0].dest).to.equal('foo.bar');
      expect(result[0].count).to.equal(1);

      expect(warns.length).to.equal(6);
      expect(warns.sort()[0]).to.match(/PINGFAIL/);
      expect(warns.sort()[0]).to.match(/http 404/i);
      expect(warns.sort()[1]).to.match(/PINGFAIL/);
      expect(warns.sort()[1]).to.match(/http 502/i);
      expect(warns.sort()[2]).to.match(/PINGRETRY/);
      expect(warns.sort()[3]).to.match(/PINGRETRY/);
      expect(warns.sort()[4]).to.match(/PINGRETRY/);
      expect(warns.sort()[5]).to.match(/PINGRETRY/);
    });
  });

});
