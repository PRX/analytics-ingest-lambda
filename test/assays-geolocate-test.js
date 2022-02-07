'use strict';

const support   = require('./support');
const geo = require('../lib/assays/geolocate');

describe('geolocate', () => {

  it('looks up geo data', () => {
    return geo.look('66.6.44.4').then(look => {
      expect(look.city).to.equal(4744870);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('20147');
      expect(look.latitude).to.equal(39.018);
      expect(look.longitude).to.equal(-77.539);
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
      expect(look.city).to.equal(4744870);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('20147');
      expect(look.latitude).to.equal(39.018);
      expect(look.longitude).to.equal(-77.539);
      expect(look.masked).to.equal('66.6.44.0');
    });
  });

  it('removes unknowns', () => {
    return geo.look('unknown,66.6.44.4').then(look => {
      expect(look.city).to.equal(4744870);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('20147');
      expect(look.latitude).to.equal(39.018);
      expect(look.longitude).to.equal(-77.539);
      expect(look.masked).to.equal('66.6.44.0');
    });
  });

  it('removes blanks', () => {
    return geo.look(', , 66.6.44.4 ,99.99.99.99').then(look => {
      expect(look.city).to.equal(4744870);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('20147');
      expect(look.latitude).to.equal(39.018);
      expect(look.longitude).to.equal(-77.539);
      expect(look.masked).to.equal('66.6.44.0');
    });
  });

  it('handles ipv6', () => {
    return geo.look('blah,blah, 2600:1f15:1:1:1:1:1:1').then(look => {
      expect(look.city).to.equal(4509177);
      expect(look.country).to.equal(6252001);
      expect(look.postal).to.equal('43215');
      expect(look.latitude).to.be.within(38, 41);
      expect(look.longitude).to.be.within(-90, -80);
      expect(look.masked).to.equal('2600:1f15:1:1:1:1:1::');
    });
  });

});
