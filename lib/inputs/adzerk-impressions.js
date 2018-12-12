'use strict';

const logger = require('../logger');
const pingurl = require('../pingurl');
const urlutil = require('../urlutil');

/**
 * Hit adzerk impression urls
 */
module.exports = class AdzerkImpressions {

  constructor(records, timeout, timeoutWait) {
    this._records = (records || []).filter(r => this.check(r));
    this._timeout = timeout;
    this._timeoutWait = timeoutWait;
  }

  check(record) {
    return record.type === 'combined'
      && (record.impressions || []).some(i => i.impressionUrl && !i.isDuplicate);
  }

  async insert() {
    if (this._records.length == 0) {
      return []
    }

    let hostCounts = {};
    let pings = [];
    this._records.forEach(r => {
      const imps = (r.impressions || []).filter(i => i.impressionUrl && !i.isDuplicate);
      imps.forEach(i => pings.push(this.ping(i.impressionUrl, r, hostCounts)));
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
      logger.warn(`PINGFAIL ${err}`);
    }
  }

}
