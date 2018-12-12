'use strict';

const logger = require('../logger');
const pingurl = require('../pingurl');
const urlutil = require('../legacy-urlutil');

/**
 * Custom pingback urls for Adzerk creatives
 */
module.exports = class LegacyAdzerkPingbacks {

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
      let pingData = [], hostCounts = {};

      // get array of pingbacks for each record
      this._records.forEach(r => {
        pingData = pingData.concat(r.pingbacks.map(pb => {
          return {record: r, url: urlutil.expand(pb, r)};
        }));
      });

      // ping in parallel
      return Promise.all(pingData.map(data => {
        return pingurl.ping(data.url, data.record, this._timeout, this._timeoutWait).then(
          result => urlutil.count(hostCounts, data.url),
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
