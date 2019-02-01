'use strict';

const support = require('./support');
const logger = require('../lib/logger');
const pingurl = require('../lib/pingurl');

describe('pingurl', () => {

  it('handles bad urls', () => {
    return pingurl.ping('foo.bar/stuff').then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/invalid ping url/i) }
    );
  });

  it('gets http urls', () => {
    let scope = nock('http://www.foo.bar').get('/the/path').reply(200);
    return pingurl.ping('http://www.foo.bar/the/path').then(resp => {
      expect(resp).to.equal(true);
      expect(scope.isDone()).to.equal(true);
    });
  });

  it('gets https urls', () => {
    let scope = nock('https://www.foo.bar').get('/the/path').reply(200);
    return pingurl.ping('https://www.foo.bar/the/path').then(resp => {
      expect(resp).to.equal(true);
      expect(scope.isDone()).to.equal(true);
    });
  });

  it('handles hostname errors', () => {
    return pingurl.ping('http://this.is.not.a.real.domain').then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/ENOTFOUND/i) }
    );
  });

  it('throws http errors', () => {
    let scope = nock('http://www.foo.bar').get('/').reply(404);
    return pingurl.ping('http://www.foo.bar/').then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/http 404 from/i) }
    );
  });

  it('retries 502 errors', () => {
    sinon.stub(logger, 'warn');
    let scope = nock('http://www.foo.bar').get('/').times(3).reply(502);
    return pingurl.ping('http://www.foo.bar/', null, undefined, 0).then(
      () => { throw new Error('Should have gotten error') },
      e => {
        expect(e.message).to.match(/http 502 from/i);
        expect(scope.isDone()).to.equal(true);
      }
    );
  });

  it('times out with a nocked delay', () => {
    nock('http://www.foo.bar').get('/timeout').delay(2000).reply(200);
    return pingurl.ping('http://www.foo.bar/timeout', null, 10).then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/http timeout from/i) }
    );
  });

  it('times out with a nocked redirect-delay', () => {
    nock('http://www.foo.bar').get('/redirect').reply(302, undefined, {Location: 'http://www.foo.bar/timeout'});
    nock('http://www.foo.bar').get('/timeout').delay(2000).reply(200);
    return pingurl.ping('http://www.foo.bar/redirect', null, 10).then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/http timeout from /i) }
    );
  });

  it('times out with an actual delay', () => {
    let url = 'http://slowwly.robertomurray.co.uk/delay/2000/url/http://dovetail.prxu.org/ping';
    return pingurl.ping(url, null, 10).then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/http timeout from/i) }
    );
  });

  it('times out with an actual redirect-delay', () => {
    let url = 'http://slowwly.robertomurray.co.uk/delay/2000/url/http://dovetail.prxu.org/ping';
    nock('http://www.foo.bar').get('/redirect').reply(302, undefined, {Location: url});
    return pingurl.ping('http://www.foo.bar/redirect', null, 10).then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/http timeout from/i) }
    );
  });

  it('parses headers from input data', () => {
    expect(pingurl.parseHeaders()).to.eql({});
    expect(pingurl.parseHeaders({remoteAgent: ''})).to.eql({});
    expect(pingurl.parseHeaders({remoteAgent: 'foo'})).to.eql({'User-Agent': 'foo'});
    expect(pingurl.parseHeaders({remoteIp: '999, 888, 777'})).to.eql({'X-Forwarded-For': '999, 888, 777'});
    expect(pingurl.parseHeaders({remoteReferrer: 'http://www.prx.org'})).to.eql({'Referer': 'http://www.prx.org'});
  });

  it('proxies headers to request', () => {
    let opts = {reqheaders: {
      'User-Agent': 'foo',
      'Referer': 'bar',
      'X-Forwarded-For': '9.8.7.6'
    }};
    let scope = nock('http://www.foo.bar', opts).get('/the/path').reply(200);
    let input = {remoteAgent: 'foo', remoteIp: '9.8.7.6', remoteReferrer: 'bar'};
    return pingurl.ping('http://www.foo.bar/the/path', input).then(resp => {
      expect(resp).to.equal(true);
      expect(scope.isDone()).to.equal(true);
    });
  });

  it('follows redirects', () => {
    let hdrs = {'Location': 'http://www.foo.bar/redirected'};
    let scope1 = nock('http://www.foo.bar').get('/redirect').reply(302, undefined, hdrs);
    let scope2 = nock('http://www.foo.bar').get('/redirected').reply(200);
    return pingurl.ping('http://www.foo.bar/redirect').then(resp => {
      expect(resp).to.equal(true);
      expect(scope1.isDone()).to.equal(true);
      expect(scope2.isDone()).to.equal(true);
    });
  });

});
