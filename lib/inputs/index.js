'use strict';

const logger = require('../logger');
const AdzerkImpressions = require('./adzerk-impressions');
const AdzerkPingbacks = require('./adzerk-pingbacks');
const DovetailDownloads = require('./dovetail-downloads');
const DovetailImpressions = require('./dovetail-impressions');

/**
 * Delegate input parsers
 */
module.exports = class Inputs {

  constructor(records, doPingbacks) {
    this._types = [];
    this._unknowns = [];

    // do EITHER pingbacks or bigquery - NOT BOTH at the same time, as the
    // kinesis retry logic is different between them.
    if (doPingbacks) {
      this._types.push(new AdzerkImpressions(records));
      this._types.push(new AdzerkPingbacks(records));
      this._types.push(new DovetailDownloads());
      this._types.push(new DovetailImpressions());
    } else {
      this._types.push(new AdzerkImpressions());
      this._types.push(new AdzerkPingbacks());
      this._types.push(new DovetailDownloads(records));
      this._types.push(new DovetailImpressions(records));
    }

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
      return results.reduce((a, b) => a.concat(b), []).map(r => {
        logger.info(`Inserted ${r.count} rows into ${r.dest}`);
        return r;
      });
    });
  }

}
