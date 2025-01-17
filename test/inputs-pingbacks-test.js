'use strict';

const support = require('./support');
const logger = require('../lib/logger');
const Pingbacks = require('../lib/inputs/pingbacks');

describe('pingbacks', () => {
  let pingbacks = new Pingbacks();

  it('recognizes pingback records', () => {
    expect(pingbacks.check({})).to.be.false;
    expect(pingbacks.check({ type: 'impression', pingbacks: ['foo'] })).to.be.false;
    expect(pingbacks.check({ impressionUrl: 'foo', isDuplicate: false })).to.be.false;
    expect(
      pingbacks.check({ type: 'postbytes', impressions: [{ pings: ['foo'], isDuplicate: true }] }),
    ).to.be.false;
    expect(pingbacks.check({ type: 'postbytes', impressions: [{ pings: ['foo'] }] })).to.be.true;
  });

  it('inserts nothing', async () => {
    expect((await pingbacks.insert()).length).to.equal(0);
  });

  it('pings postbyte records', async () => {
    const infos = [];
    sinon.stub(logger, 'info').callsFake((msg, args) => infos.push({ msg, args }));

    nock('http://www.foo.bar').get('/ping1').reply(200);
    nock('https://www.foo.bar').get('/ping2/11').reply(200);
    nock('http://bar.foo').get('/ping3').reply(200);
    nock('https://www.foo.bar')
      .get('/ping4?url=dovetail.prxu.org%2Fabc%2Ftheguid%2Fpath.mp3%3Ffoo%3Dbar')
      .reply(200);
    nock('http://www.foo.bar').get('/ping5/55').reply(200);

    pingbacks = new Pingbacks([
      {
        type: 'postbytes',
        impressions: [
          {
            adId: 11,
            isDuplicate: false,
            pings: ['http://www.foo.bar/ping1', 'https://www.foo.bar/ping2/{ad}'],
          },
          { adId: 22, isDuplicate: true, pings: ['https://www.foo.bar/ping3'] },
        ],
      },
      {
        type: 'postbytes',
        impressions: [
          { adId: 33, isDuplicate: false, pings: ['http://bar.foo/ping3'] },
          {
            adId: 44,
            url: '/abc/theguid/path.mp3?foo=bar',
            isDuplicate: false,
            pings: ['https://www.foo.bar/ping4{?url}'],
          },
          { adId: 55, isDuplicate: false, pings: ['http://www.foo.bar/ping5/{ad}'] },
          { adId: 66, isDuplicate: false, pings: [] },
        ],
      },
    ]);
    const result = await pingbacks.insert();
    expect(result.length).to.equal(2);
    expect(result.map(r => r.dest).sort()).to.eql(['bar.foo', 'www.foo.bar']);
    expect(result.find(r => r.dest === 'bar.foo').count).to.equal(1);
    expect(result.find(r => r.dest === 'www.foo.bar').count).to.equal(4);

    expect(infos.length).to.equal(5);
    expect(infos.map(i => i.msg)).to.eql(['PINGED', 'PINGED', 'PINGED', 'PINGED', 'PINGED']);
    expect(infos.map(i => i.args.url).sort()).to.eql([
      'http://bar.foo/ping3',
      'http://www.foo.bar/ping1',
      'http://www.foo.bar/ping5/55',
      'https://www.foo.bar/ping2/11',
      'https://www.foo.bar/ping4?url=dovetail.prxu.org%2Fabc%2Ftheguid%2Fpath.mp3%3Ffoo%3Dbar',
    ]);
  });

  it('complains about failed pings', async () => {
    nock('http://foo.bar').get('/ping1').reply(404);
    nock('http://foo.bar').get('/ping2').times(2).reply(502);
    nock('http://foo.bar').get('/ping2').reply(200);
    nock('http://bar.foo').get('/ping3').times(3).reply(502);

    pingbacks = new Pingbacks(
      [
        {
          type: 'postbytes',
          impressions: [{ isDuplicate: false, pings: ['http://foo.bar/ping1'] }],
        },
        {
          type: 'postbytes',
          impressions: [
            { isDuplicate: false, pings: ['http://foo.bar/ping2', 'http://bar.foo/ping3'] },
          ],
        },
        {
          type: 'postbytes',
          impressions: [
            {
              isDuplicate: false,
              pings: ['https://pingback.podtrac.com/<showid>/<adid>/<pos>?IP={ip}'],
            },
          ],
        },
      ],
      1000,
      0,
    );

    let infos = [],
      warns = [],
      errors = [];
    sinon.stub(logger, 'info').callsFake(msg => infos.push(msg));
    sinon.stub(logger, 'warn').callsFake(msg => warns.push(msg));
    sinon.stub(logger, 'error').callsFake(msg => errors.push(msg));

    const result = await pingbacks.insert();
    expect(result.length).to.equal(1);
    expect(result[0].dest).to.equal('foo.bar');
    expect(result[0].count).to.equal(1);
    expect(infos).to.eql(['PINGED']);

    expect(warns.length).to.equal(7);
    expect(warns.sort()[0]).to.match(/PINGFAIL/);
    expect(warns.sort()[0]).to.match(/http 404/i);
    expect(warns.sort()[1]).to.match(/PINGFAIL/);
    expect(warns.sort()[1]).to.match(/http 502/i);
    expect(warns.sort()[2]).to.match(/PINGFAIL/);
    expect(warns.sort()[2]).to.match(/invalid literal/i);
    expect(warns.sort()[3]).to.match(/PINGRETRY/);
    expect(warns.sort()[4]).to.match(/PINGRETRY/);
    expect(warns.sort()[5]).to.match(/PINGRETRY/);
    expect(warns.sort()[6]).to.match(/PINGRETRY/);

    expect(errors.length).to.equal(0);
  });

  it('does not ping duplicate records', async () => {
    nock('http://foo.bar').get('/ping2').reply(200);
    sinon.stub(logger, 'info');
    sinon.stub(logger, 'warn');
    pingbacks = new Pingbacks([
      {
        type: 'postbytes',
        remoteReferrer: 'http://cav.is/domain/threat',
        impressions: [
          { adId: 11, isDuplicate: true, cause: 'domainthreat', pings: ['http://foo.bar/ping1'] },
          { adId: 22, pings: ['http://foo.bar/ping2'] },
        ],
      },
    ]);
    expect(pingbacks._records.length).to.equal(1);

    const result = await pingbacks.insert();
    expect(result.length).to.equal(1);
    expect(logger.warn).not.to.have.been.called;
  });
});
