'use strict';

const logger = require('../logger');
const pingurl = require('../pingurl');
const urlutil = require('../legacy-urlutil');

/**
 * Hit adzerk impression urls
 */
module.exports = class LegacyAdzerkImpressions {

  constructor(records, timeout, timeoutWait) {
    this._records = (records || []).filter(r => this.check(r));
    this._timeout = timeout;
    this._timeoutWait = timeoutWait;
  }

  check(record) {
    return !!record.impressionUrl && !record.isDuplicate;
  }

  insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    } else {
      let hostCounts = {};
      return Promise.all(this._records.map(r => {
        return pingurl.ping(r.impressionUrl, r, this._timeout, this._timeoutWait).then(
          result => urlutil.count(hostCounts, r.impressionUrl),
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
