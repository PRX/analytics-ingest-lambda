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
    return Array.isArray(record.pingbacks) && !record.isDuplicate;
  }

  insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    } else {
      let pingUrls = [], hostCounts = {};

      // get array of pingback urls for each record
      this._records.forEach(r => {
        pingUrls = pingUrls.concat(r.pingbacks.map(pb => urlutil.expand(pb, r)));
      });

      // ping in parallel
      return Promise.all(pingUrls.map(pingUrl => {
        return pingurl.ping(pingUrl, this._timeout, this._timeoutWait).then(
          result => urlutil.count(hostCounts, pingUrl),
          err => logger.warn(`PINGFAIL ${err}`)
        );
      })).then(() => {
        return Object.keys(hostCounts).map(host => {
          return {dest: host, count: hostCounts[host]};
        });
      });
    }
  }

}
