'use strict';

const support = require('./support');
const center = require('../lib/assays/datacenter');

describe('datacenters', () => {
  const NULL = { start: null, end: null, provider: null };

  it('returns nulls for bad strings', async () => {
    expect(await center.look('')).to.eql(NULL);
    expect(await center.look('nada')).to.eql(NULL);
    expect(await center.look('999.999.999.999')).to.eql(NULL);
    expect(await center.look('2401:6500:ff00:0::a::')).to.eql(NULL);
  });

  it('returns nulls for non-matches', async () => {
    expect(await center.look('127.0.0.1')).to.eql(NULL);
    expect(await center.look('3.5.127.255')).to.eql(NULL);
    expect(await center.look('3.33.102.102')).to.eql(NULL);
    expect(await center.look('2401:6500:ff00::')).to.eql(NULL);
  });

  // AWS 3.5.128.0 - 3.5.169.255
  it('matches the boundaries of v4 datacenters', async () => {
    expect(await center.look('3.5.127.255')).to.eql(NULL);
    expect(await center.look('3.5.128.0')).not.to.eql(NULL);
    expect(await center.look('3.5.130.33')).not.to.eql(NULL);
    expect(await center.look('3.5.169.255')).not.to.eql(NULL);
    expect(await center.look('3.5.170.0')).to.eql(NULL);
  });

  // AWS 2400:6500:ff00:: - 2400:6500:ff00::ffff:ffff:ffff:ffff
  it('matches the boundaries of v6 datacenters', async () => {
    expect(await center.look('2400:6500:ff00::1",')).to.eql(NULL);
    expect(await center.look('2400:6500:ff00::')).not.to.eql(NULL);
    expect(await center.look('2400:6500:ff00:0000:abcd::')).not.to.eql(NULL);
    expect(await center.look('2400:6500:ff00:0:ffff:ffff:ffff:ffff')).not.to.eql(NULL);
    expect(await center.look('2400:6500:ff00:1::1')).to.eql(NULL);
  });
});
