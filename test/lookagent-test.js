'use strict';

const support = require('./support');
const lookagent = require('../lib/lookagent');

describe('lookagent', () => {

  it('looks up agent strings', () => {
    return lookagent.look('Stitcher/Android').then(look => {
      expect(look.name).to.equal(23);
      expect(look.type).to.equal(36);
      expect(look.os).to.equal(42);
    });
  });

  it('handle partial matching agents', () => {
    return lookagent.look('Mozilla/5.0 (Linux; U; Android 6.0.1;px5/MXC89L)').then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.equal(40);
      expect(look.os).to.equal(42);
    });
  });

  it('handles blanks', () => {
    return lookagent.look('').then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.be.null;
      expect(look.os).to.be.null;
    });
  });

  it('handle nonsense', () => {
    return lookagent.look('ontehunteho').then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.be.null;
      expect(look.os).to.be.null;
    });
  });

  it('respects blanks', () => {
    let str = 'Downcast/2.9.16 (iPhone; iOS 10.3.2; Scale/2.00)';
    return lookagent.look(` ${str}`).then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.be.null;
      expect(look.os).to.be.null;
      return lookagent.look(`${str}  `);
    }).then(look => {
      expect(look.name).to.equal(11);
      expect(look.type).to.equal(36);
      expect(look.os).to.equal(43);
    });
  });

});
