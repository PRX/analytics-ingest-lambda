'use strict';

const logger = require('../logger');
const AdzerkImpressions = require('./adzerk-impressions');
const AdzerkPingbacks = require('./adzerk-pingbacks');
const DovetailDownloads = require('./dovetail-downloads');
const DovetailImpressions = require('./dovetail-impressions');
const RedisIncrements = require('./redis-increments');

/**
 * Delegate input parsers
 */
class Inputs {

  constructor(records/*, types */) {
    this._types = [].slice.call(arguments, 1);
    this._unknowns = [];

    // check for unrecognized records
    const allTypes = [
      new AdzerkImpressions(),
      new AdzerkPingbacks(),
      new DovetailDownloads(),
      new DovetailImpressions(),
      new RedisIncrements()
    ];
    records.forEach(r => {
      if (!allTypes.some(t => t.check(r))) {
        this._unknowns.push(r);
      }
    });
  }

  get unrecognized() {
    return this._unknowns;
  }

  insertAll() {
    if (this._types.length) {
      return Promise.all(this._types.map(t => t.insert())).then(results => {
        return results.reduce((a, b) => a.concat(b), []).map(r => {
          logger.info(`Inserted ${r.count} rows into ${r.dest}`);
          return r;
        });
      });
    } else {
      return Promise.resolve([]);
    }
  }

}

/**
 * Actual input groupings
 */
module.exports.BigqueryInputs = class BigqueryInputs extends Inputs {
  constructor(records) {
    super(records, new DovetailDownloads(records), new DovetailImpressions(records));
  }
}
module.exports.PingbackInputs = class PingbackInputs extends Inputs {
  constructor(records) {
    super(records, new AdzerkImpressions(records), new AdzerkPingbacks(records));
  }
}
module.exports.RedisInputs = class RedisInputs extends Inputs {
  constructor(records) {
    super(records, new RedisIncrements(records));
  }
}
