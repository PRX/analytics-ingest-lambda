'use strict';

const support = require('./support');
const agent = require('../lib/assays/useragent');

describe('useragent', () => {

  it('looks up agent strings', () => {
    return agent.look('Stitcher/Android').then(look => {
      expect(look.name).to.equal(23);
      expect(look.type).to.equal(36);
      expect(look.os).to.equal(42);
      expect(look.bot).to.equal(false);
    });
  });

  it('handle partial matching agents', () => {
    return agent.look('Mozilla/5.0 (Linux; U; Android 6.0.1;px5/MXC89L)').then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.equal(40);
      expect(look.os).to.equal(42);
      expect(look.bot).to.equal(false);
    });
  });

  it('handles blanks', () => {
    return agent.look('').then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.be.null;
      expect(look.os).to.be.null;
      expect(look.bot).to.equal(false);
    });
  });

  it('handle nonsense', () => {
    return agent.look('ontehunteho').then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.be.null;
      expect(look.os).to.be.null;
      expect(look.bot).to.equal(false);
    });
  });

  it('handle bots', () => {
    return agent.look('googlebot').then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.be.null;
      expect(look.os).to.be.null;
      expect(look.bot).to.equal(true);
    });
  });

  it('respects blanks', () => {
    let str = 'Downcast/2.9.16 (iPhone; iOS 10.3.2; Scale/2.00)';
    return agent.look(` ${str}`).then(look => {
      expect(look.name).to.be.null;
      expect(look.type).to.be.null;
      expect(look.os).to.be.null;
      expect(look.bot).to.equal(false);
      return agent.look(`${str}  `);
    }).then(look => {
      expect(look.name).to.equal(11);
      expect(look.type).to.equal(36);
      expect(look.os).to.equal(43);
      expect(look.bot).to.equal(false);
    });
  });

});
