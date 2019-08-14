'use strict';

const support = require('./support');
const iputil = require('../lib/iputil');

describe('iputil', () => {

  it('cleans ips', () => {
    expect(iputil.clean('')).to.equal(undefined);
    expect(iputil.clean(',blah')).to.equal(undefined);
    expect(iputil.clean(',99.99.99.99')).to.equal('99.99.99.99');
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
    expect(iputil.mask('2804:18:1012:6b65:1:3:3561:14b8')).to.equal('2804:18:1012:6b65:1:3:3561::');
  });

  it('masks the leftmost x-forwarded-for ip', () => {
    expect(iputil.maskLeft('66.6.44.4, 99.99.99.99')).to.equal('66.6.44.0, 99.99.99.99');
    expect(iputil.maskLeft('unknown, 99.99.99.99, 127.0.0.1')).to.equal('unknown, 99.99.99.99, 127.0.0.1');
  });

  it('converts to fixed length strings', () => {
    expect(iputil.fixed('blah')).to.equal('blah');
    expect(iputil.fixed('1234.5678.1234.5678')).to.equal('1234.5678.1234.5678');
    expect(iputil.fixed('192.68.0.1')).to.equal('192.068.000.001');
    expect(iputil.fixed('2804:18:1012::61:14b8')).to.equal('2804:0018:1012:0000:0000:0000:0061:14b8');
  });

  it('converts to fixed length strings and returns the kind of ip', () => {
    expect(iputil.fixedKind('blah')).to.eql(['blah', null]);
    expect(iputil.fixedKind('1234.5678.1234.5678')).to.eql(['1234.5678.1234.5678', null]);
    expect(iputil.fixedKind('192.68.0.1')).to.eql(['192.068.000.001', 'v4']);
    expect(iputil.fixedKind('2804:18:1012::61:14b8')).to.eql(['2804:0018:1012:0000:0000:0000:0061:14b8', 'v6']);
  });

});
