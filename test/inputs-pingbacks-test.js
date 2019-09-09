'use strict';

const support = require('./support');
const logger = require('../lib/logger');
const Pingbacks = require('../lib/inputs/pingbacks');

describe('pingbacks', () => {

  let pingbacks = new Pingbacks();

  it('recognizes pingback records', () => {
    expect(pingbacks.check({})).to.be.false;
    expect(pingbacks.check({type: 'impression', pingbacks: ['foo']})).to.be.false;
    expect(pingbacks.check({impressionUrl: 'foo', isDuplicate: false})).to.be.false;
    expect(pingbacks.check({type: 'combined', impressions: [{pings: []}]})).to.be.false;
    expect(pingbacks.check({type: 'combined', impressions: [{pings: ['foo'], isDuplicate: true}]})).to.be.false;
    expect(pingbacks.check({type: 'combined', impressions: [{pings: ['foo']}]})).to.be.true;
    expect(pingbacks.check({type: 'combined', impressions: [{}, {pings: ['foo']}]})).to.be.true;
    expect(pingbacks.check({type: 'postbytes', impressions: [{pings: ['foo'], isDuplicate: true}]})).to.be.false;
    expect(pingbacks.check({type: 'postbytes', impressions: [{pings: ['foo']}]})).to.be.true;
  });

  it('inserts nothing', async () => {
    expect((await pingbacks.insert()).length).to.equal(0)
  });

  it('pings combined records', async () => {
    nock('http://www.foo.bar').get('/ping1').reply(200);
    nock('https://www.foo.bar').get('/ping2/11').reply(200);
    nock('http://bar.foo').get('/ping3').reply(200);
    nock('https://www.foo.bar').get('/ping4?url=dovetail.prxu.org%2Fabc%2Ftheguid%2Fpath.mp3%3Ffoo%3Dbar').reply(200);
    nock('http://www.foo.bar').get('/ping5/55').reply(200);

    pingbacks = new Pingbacks([
      {type: 'combined', impressions: [
        {adId: 11, isDuplicate: false, pings: ['http://www.foo.bar/ping1', 'https://www.foo.bar/ping2/{ad}']},
        {adId: 22, isDuplicate: true,  pings: ['https://www.foo.bar/ping3']},
      ]},
      {type: 'combined', impressions: [
        {adId: 33, isDuplicate: false, pings: ['http://bar.foo/ping3']},
        {adId: 44, url: '/abc/theguid/path.mp3?foo=bar', isDuplicate: false, pings: ['https://www.foo.bar/ping4{?url}']},
        {adId: 55, isDuplicate: false, pings: ['http://www.foo.bar/ping5/{ad}']},
        {adId: 66, isDuplicate: false, pings: []}
      ]}
    ]);
    const result = await pingbacks.insert();
    expect(result.length).to.equal(2);
    expect(result.map(r => r.dest).sort()).to.eql(['bar.foo', 'www.foo.bar']);
    expect(result.find(r => r.dest === 'bar.foo').count).to.equal(1);
    expect(result.find(r => r.dest === 'www.foo.bar').count).to.equal(4);
  });

  it('complains about failed pings', async () => {
    nock('http://foo.bar').get('/ping1').reply(404);
    nock('http://foo.bar').get('/ping2').times(2).reply(502);
    nock('http://foo.bar').get('/ping2').reply(200);
    nock('http://bar.foo').get('/ping3').times(3).reply(502);

    pingbacks = new Pingbacks([
      {type: 'combined', impressions: [
        {isDuplicate: false, pings: ['http://foo.bar/ping1']}
      ]},
      {type: 'combined', impressions: [
        {isDuplicate: false, pings: ['http://foo.bar/ping2', 'http://bar.foo/ping3']}
      ]}
    ], 1000, 0);

    let warns = [];
    sinon.stub(logger, 'warn').callsFake(msg => warns.push(msg));

    const result = await pingbacks.insert();
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

  it('does not ping duplicate records', async () => {
    sinon.stub(logger, 'warn');
    pingbacks = new Pingbacks([
      {
        type: 'combined',
        remoteAgent: 'googlebot',
        impressions: [{adId: 11, isDuplicate: false, pings: ['http://foo.bar/ping1']}]
      }
    ]);
    expect(pingbacks._records.length).to.equal(1);

    const result = await pingbacks.insert();
    expect(result.length).to.equal(0);
    expect(logger.warn).not.to.have.been.called;
  })

});
