'use strict';

const support   = require('./support');
const geo = require('../lib/assays/geolocate');

describe('geolocate', () => {

  it('looks up geo data', () => {
    return geo.look('66.6.44.4').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('10010');
      expect(look.latitude).to.equal(40.738);
      expect(look.longitude).to.equal(-73.9858);
      expect(look.masked).to.equal('66.6.44.0');
    });
  });

  it('handle unlocate-able ips', () => {
    return geo.look('192.168.0.1').then(look => {
      expect(look.city).to.be.null;
      expect(look.country).to.be.null;
      expect(look.postal).to.be.null;
      expect(look.latitude).to.be.null;
      expect(look.longitude).to.be.null;
      expect(look.masked).to.equal('192.168.0.0');
    });
  });

  it('handle blanks', () => {
    return geo.look('').then(look => {
      expect(look.city).to.be.null;
      expect(look.country).to.be.null;
      expect(look.postal).to.be.null;
      expect(look.latitude).to.be.null;
      expect(look.longitude).to.be.null;
      expect(look.masked).to.be.null;
    });
  });

  it('handle nonsense', () => {
    return geo.look('ontehunteho').then(look => {
      expect(look.city).to.be.null;
      expect(look.country).to.be.null;
      expect(look.postal).to.be.null;
      expect(look.latitude).to.be.null;
      expect(look.longitude).to.be.null;
      expect(look.masked).to.be.null;
    });
  });

  it('splits forwarded-for ips', () => {
    return geo.look('66.6.44.4, 99.99.99.99, 127.0.0.1').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('10010');
      expect(look.latitude).to.equal(40.738);
      expect(look.longitude).to.equal(-73.9858);
      expect(look.masked).to.equal('66.6.44.0');
    });
  });

  it('removes unknowns', () => {
    return geo.look('unknown,66.6.44.4').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('10010');
      expect(look.latitude).to.equal(40.738);
      expect(look.longitude).to.equal(-73.9858);
      expect(look.masked).to.equal('66.6.44.0');
    });
  });

  it('removes blanks', () => {
    return geo.look(', , 66.6.44.4 ,99.99.99.99').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('10010');
      expect(look.latitude).to.equal(40.738);
      expect(look.longitude).to.equal(-73.9858);
      expect(look.masked).to.equal('66.6.44.0');
    });
  });

  it('handles ipv6', () => {
    return geo.look('blah,blah, 2804:18:1012:6b65:1:3:3561:14b8').then(look => {
      expect(look.city).to.equal(3448439);
      expect(look.country).to.equal(3469034);
      expect(look.postal).to.equal('01000');
      expect(look.latitude).to.be.within(-24, -23);
      expect(look.longitude).to.be.within(-47, -46);
      expect(look.masked).to.equal('2804:18:1012:6b65:1:3:3561:0');
    });
  });

});
