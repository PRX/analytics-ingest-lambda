'use strict';

const url = require('url');
const logger = require('../logger');
const pingurl = require('../pingurl');

/**
 * Hit adzerk impression urls
 */
module.exports = class AdzerkImpressions {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
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
        let host = url.parse(r.impressionUrl).host;
        hostCounts[host] = hostCounts[host] || 0;
        return pingurl.ping(r.impressionUrl).then(
          result => hostCounts[host]++,
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
