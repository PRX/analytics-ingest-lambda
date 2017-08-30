'use strict';

const DovetailDownloads = require('./dovetail-downloads');
const DovetailImpressions = require('./dovetail-impressions');

/**
 * Delegate input parsers
 */
module.exports = class Inputs {

  constructor(records) {
    this._types = [
      new DovetailDownloads(records),
      new DovetailImpressions(records)
    ];
    this._unknowns = [];

    // find records not recognized by any input type
    if (records) {
      records.forEach(r => {
        if (!this._types.some(t => t.check(r))) {
          this._unknowns.push(r);
        }
      });
    }
  }

  get unrecognized() {
    return this._unknowns;
  }

  insertAll() {
    return Promise.all(this._types.map(t => t.insert())).then(results => {
      return results.reduce((a, b) => a.concat(b), []);
    });
  }

}
