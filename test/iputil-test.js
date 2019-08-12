'use strict';

const support = require('./support');
const iputil = require('../lib/iputil');

describe('iputil', () => {

  it('cleans ips', () => {
    expect(iputil.clean('')).to.equal(undefined);
    expect(iputil.clean(',blah')).to.equal(undefined);
    expect(iputil.clean(',999.999.999.999')).to.equal('999.999.999.999');
    expect(iputil.clean(', , 66.6.44.4 ,99.99.99.99')).to.equal('66.6.44.4');
  });

  it('cleans a list of ips', () => {
    expect(iputil.cleanAll('')).to.equal(undefined);
    expect(iputil.cleanAll(',blah')).to.equal(undefined);
    expect(iputil.cleanAll(', , 66.6.44.4 ,99.99.99.99, blah')).to.equal('66.6.44.4, 99.99.99.99');
  });

  it('masks ips', () => {
    expect(iputil.mask('blah')).to.equal('blah');
    expect(iputil.mask('1234.5678.1234.5678')).to.equal('1234.5678.1234.5678');
    expect(iputil.mask('192.168.0.1')).to.equal('192.168.0.0');
    expect(iputil.mask('2804:18:1012:6b65:1:3:3561:14b8')).to.equal('2804:18:1012:6b65:1:3:3561:0');
  });

  it('masks the leftmost x-forwarded-for ip', () => {
    expect(iputil.maskLeft('66.6.44.4, 99.99.99.99')).to.equal('66.6.44.0, 99.99.99.99');
    expect(iputil.maskLeft('unknown, 99.99.99.99, 127.0.0.1')).to.equal('unknown, 99.99.99.99, 127.0.0.1');
  });

});
