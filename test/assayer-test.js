'use strict';

const support = require('./support');
const assayer = require('../lib/assayer');

describe('assayer', () => {
  describe('test', () => {
    it('returns basic info', async () => {
      const info = await assayer.test({ remoteAgent: 'something' });
      expect(info).to.eql({
        isDuplicate: false,
        cause: null,
        agent: { name: null, type: null, os: null, bot: false },
      });
    });

    it('checks the download duplicate', async () => {
      const info = await assayer.test({
        download: { isDuplicate: true, cause: 'foo' },
        remoteAgent: 'googlebot',
      });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('foo');
    });

    it('has a default cause', async () => {
      const info = await assayer.test({
        download: { isDuplicate: true },
        remoteAgent: 'googlebot',
      });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('unknown');
    });

    it('checks for bots', async () => {
      const info = await assayer.test({ remoteAgent: 'googlebot' });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('bot');
    });

    it('checks for domain threats', async () => {
      const info = await assayer.test({ remoteReferrer: 'http://cav.is/any/thing' });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('domainthreat');
    });

    it('checks for datacenters', async () => {
      const info = await assayer.test({ remoteIp: '3.1.87.65' });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('datacenter: Amazon AWS');
    });
  });

  describe('testImpression', async () => {
    it('returns basic info', async () => {
      const info = await assayer.testImpression({ remoteAgent: 'something' }, {});
      expect(info).to.eql({
        isDuplicate: false,
        cause: null,
        agent: { name: null, type: null, os: null, bot: false },
      });
    });

    it('checks the download duplicate', async () => {
      const info = await assayer.testImpression(
        { remoteAgent: 'googlebot' },
        { isDuplicate: true, cause: 'foo' },
      );
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('foo');
    });

    it('has a default cause', async () => {
      const info = await assayer.testImpression(
        { remoteAgent: 'googlebot' },
        { isDuplicate: true },
      );
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('unknown');
    });

    it('checks for bots', async () => {
      const info = await assayer.testImpression(
        { remoteAgent: 'googlebot' },
        { isDuplicate: false },
      );
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('bot');
    });

    it('checks for datacenters', async () => {
      const info = await assayer.testImpression({ remoteIp: '3.1.87.65' }, { isDuplicate: false });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('datacenter: Amazon AWS');
    });
  });
});
