'use strict';

const logger = require('../logger');
const assayer = require('../assayer');
const pingurl = require('../pingurl');
const urlutil = require('../urlutil');

/**
 * Ping Adzerk impression urls and 3rd party pingbacks
 */
module.exports = class Pingbacks {

  constructor(records, timeout, timeoutWait) {
    this._records = (records || []).filter(r => this.check(r));
    this._timeout = timeout;
    this._timeoutWait = timeoutWait;
  }

  check(record) {
    if (record.type === 'combined' || record.type === 'postbytes') {
      return (record.impressions || []).some(i => i.pings && i.pings.length && !i.isDuplicate);
    } else {
      return false;
    }
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // filter duplicates
    let hostCounts = {};
    let pings = [];
    await this.eachImpression(async (r, i) => {
      const {isDuplicate} = await assayer.testImpression(r, i);
      if (!isDuplicate) {
        (i.pings || []).forEach(pb => {
          try {
            const url = urlutil.expand(pb, {...r, ...i});
            pings.push(this.ping(url, r, hostCounts));
          } catch (err) {
            logger.error(`PINGFAIL ${err}`, {url: pb});
          }
        });
      }
    });

    // ping urls, incrementing the counts per-host
    await Promise.all(pings);
    return Object.keys(hostCounts).map(host => {
      return {dest: host, count: hostCounts[host]};
    });
  }

  async ping(url, record, hostCounts) {
    try {
      await pingurl.ping(url, record, this._timeout, this._timeoutWait);
      urlutil.count(hostCounts, url);
    } catch (err) {
      logger.warn(`PINGFAIL ${err}`, {url});
    }
  }

  async eachImpression(handler) {
    await Promise.all(this._records.map(async (rec) => {
      await Promise.all(rec.impressions.map(async (imp) => {
        await handler(rec, imp);
      }));
    }));
  }

};
