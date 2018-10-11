'use strict';

const logger = require('../logger');
const AdzerkImpressions = require('./adzerk-impressions');
const AdzerkPingbacks = require('./adzerk-pingbacks');
const DovetailBytes = require('./dovetail-bytes');
const DovetailDownloads = require('./dovetail-downloads');
const DovetailImpressions = require('./dovetail-impressions');
const RedisIncrements = require('./redis-increments');

/**
 * Abstract "Inputs" parser class.  Groups together input-types, and insert()s
 * them in parallel.  Can be called with any number of input-types, as defined
 * in the actual class exports at the bottom of this file.
 */
class Inputs {

  constructor(records) {
    this._types = [].slice.call(arguments, 1);
    this._unknowns = [];

    // check for unrecognized records
    const allTypes = [
      new AdzerkImpressions(),
      new AdzerkPingbacks(),
      new DovetailBytes(),
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
    super(
      records,
      new DovetailDownloads(records),
      new DovetailImpressions(records),
      new DovetailBytes(records)
    );
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
