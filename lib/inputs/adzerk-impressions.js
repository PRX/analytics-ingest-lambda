'use strict';

const url = require('url');
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
      return Promise.all(this._records.map(r => {
        return pingurl.ping(r.impressionUrl).then(result => {
          return url.parse(r.impressionUrl).host;
        });
      })).then(results => {
        return results.sort().reduce((acc, r) => {
          if (acc.length && acc[acc.length - 1].dest === r) {
            acc[acc.length - 1].count++;
          } else {
            acc.push({dest: r, count: 1});
          }
          return acc;
        }, []);
      });
    }
  }

}
