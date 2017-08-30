'use strict';

const support = require('./support');
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
    let scope = nock('http://www.foo.bar').get('/').times(3).reply(502);
    return pingurl.ping('http://www.foo.bar/').then(
      () => { throw new Error('Should have gotten error') },
      e => {
        expect(e.message).to.match(/http 502 from/i);
        expect(scope.isDone()).to.equal(true);
      }
    );
  });

  it('times out', () => {
    // TODO: nock doesn't seem to be able to trigger timeouts correctly
    // let scope = nock('http://www.foo.bar').get('/').delay(5000).reply(200);
    return pingurl.ping('http://deelay.me/1000/http://www.prx.org', 100).then(
      () => { throw new Error('Should have gotten error') },
      e => { expect(e.message).to.match(/http timeout from/i) }
    );
  });

});
