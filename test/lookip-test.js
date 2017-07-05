'use strict';

const support   = require('./support');
const lookip = require('../lib/lookip');

describe('lookip', () => {

  it('looks up geoname ids for city and country', () => {
    return lookip.look('66.6.44.4').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
    });
  });

  it('handle unlocate-able ips', () => {
    return lookip.look('192.168.0.1').then(look => {
      expect(look.city).to.be.null;
      expect(look.country).to.be.null;
    });
  });

  it('handle blanks', () => {
    return lookip.look('').then(look => {
      expect(look.city).to.be.null;
      expect(look.country).to.be.null;
    });
  });

  it('handle nonsense', () => {
    return lookip.look('ontehunteho').then(look => {
      expect(look.city).to.be.null;
      expect(look.country).to.be.null;
    });
  });

  it('splits forwarded-for ips', () => {
    return lookip.look('66.6.44.4, 99.99.99.99, 127.0.0.1').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
    });
  });

  it('removes unknowns', () => {
    return lookip.look('unknown,66.6.44.4').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
    });
  });

  it('removes blanks', () => {
    return lookip.look(', , 66.6.44.4 ,99.99.99.99').then(look => {
      expect(look.city).to.equal(5128581);
      expect(look.country).to.equal(6252001);
    });
  });

});
