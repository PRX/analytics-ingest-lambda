'use strict';

const logger = require('../logger');
const pingurl = require('../pingurl');
const urlutil = require('../urlutil');

/**
 * Custom pingback urls for Adzerk creatives
 */
module.exports = class AdzerkPingbacks {

  constructor(records, timeout, timeoutWait) {
    this._records = (records || []).filter(r => this.check(r));
    this._timeout = timeout;
    this._timeoutWait = timeoutWait;
  }

  check(record) {
    if (record.type === 'combined') {
      return (record.impressions || []).some(i => i.pingbacks && i.pingbacks.length && !i.isDuplicate);
    } else {
      return Array.isArray(record.pingbacks) && !record.isDuplicate;
    }
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // TODO: remove legacy 'impression' type
    let hostCounts = {};
    let pings = [];
    this._records.forEach(r => {
      if (r.type === 'combined') {
        r.impressions.filter(i => !i.isDuplicate).forEach(i => {
          (i.pingbacks || []).forEach(pb => {
            const url = urlutil.expand(pb, {...r, ...i});
            pings.push(this.ping(url, r, hostCounts));
          });
        });
      } else {
        (r.pingbacks || []).forEach(pb => {
          const url = urlutil.expand(pb, r);
          pings.push(this.ping(url, r, hostCounts));
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
      logger.warn(`PINGFAIL ${err}`);
    }
  }

}
