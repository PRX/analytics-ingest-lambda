'use strict';

const support = require('./support');
const assayer = require('../lib/assayer');

const KNOWN_DATACENTER_IP = '3.1.87.65';

describe('assayer', () => {
  describe('test', () => {
    it('returns basic info', async () => {
      const info = await assayer.test({ remoteAgent: 'something' });
      expect(info).to.eql({
        isDuplicate: false,
        cause: null,
      });
    });

    it('checks the download duplicate', async () => {
      const info = await assayer.test({
        download: { isDuplicate: true, cause: 'foo' },
        remoteIp: KNOWN_DATACENTER_IP,
      });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('foo');
    });

    it('has a default cause', async () => {
      const info = await assayer.test({
        download: { isDuplicate: true },
        remoteIp: KNOWN_DATACENTER_IP,
      });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('unknown');
    });

    it('checks for domain threats', async () => {
      const info = await assayer.test({ remoteReferrer: 'http://cav.is/any/thing' });
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('domainthreat');
    });

    it('checks for datacenters', async () => {
      const info = await assayer.test({ remoteIp: KNOWN_DATACENTER_IP });
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
      });
    });

    it('checks the download duplicate', async () => {
      const info = await assayer.testImpression(
        { remoteIp: KNOWN_DATACENTER_IP },
        { isDuplicate: true, cause: 'foo' },
      );
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('foo');
    });

    it('has a default cause', async () => {
      const info = await assayer.testImpression(
        { remoteIp: KNOWN_DATACENTER_IP },
        { isDuplicate: true },
      );
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('unknown');
    });

    it('checks for datacenters', async () => {
      const info = await assayer.testImpression(
        { remoteIp: KNOWN_DATACENTER_IP },
        { isDuplicate: false },
      );
      expect(info.isDuplicate).to.equal(true);
      expect(info.cause).to.equal('datacenter: Amazon AWS');
    });
  });
});
