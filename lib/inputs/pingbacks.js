'use strict';

const logger = require('../logger');
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

    let hostCounts = {};
    let pings = [];
    this._records.forEach(r => {
      r.impressions.filter(i => !i.isDuplicate).forEach(i => {
        (i.pings || []).forEach(pb => {
          const url = urlutil.expand(pb, {...r, ...i});
          pings.push(this.ping(url, r, hostCounts));
        });
      });
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

};
