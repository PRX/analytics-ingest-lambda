'use strict';

const support = require('./support');
const threat = require('../lib/assays/domainthreat');

describe('domainthreat', () => {

  it('returns nulls for bad strings', async () => {
    expect(await threat.look()).to.be.false;
    expect(await threat.look(false)).to.be.false;
    expect(await threat.look(false)).to.be.false;
    expect(await threat.look('')).to.be.false;
    expect(await threat.look('anything')).to.be.false;
    expect(await threat.look('87$&@*$%')).to.be.false;
  });

  it('returns false for non matching domains', async () => {
    expect(await threat.look('https://www.anywhere.com/radio')).to.be.false;
    expect(await threat.look('https://www.cav.is/radio')).to.be.false;
  });

  it('returns true for matching domains', async () => {
    expect(await threat.look('https://cav.is/radio')).to.be.true;
    expect(await threat.look('http://cav.is/radio')).to.be.true;
    expect(await threat.look('http://cav.is/')).to.be.true;
    expect(await threat.look('http://cav.is')).to.be.true;
  });

});
