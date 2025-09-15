import logger from "../logger";
import DovetailDownloads from "./dovetail-downloads";
import DovetailFrequency from "./dovetail-frequency";
import DovetailImpressions from "./dovetail-impressions";
import DynamodbData from "./dynamodb-data";
import FlightIncrements from "./flight-increments";
import Pingbacks from "./pingbacks";

/**
 * Abstract "Inputs" parser class.  Groups together input-types, and insert()s
 * them in parallel.  Can be called with any number of input-types, as defined
 * in the actual class exports at the bottom of this file.
 */
class Inputs {
  constructor(records, ...args) {
    this._types = [].slice.call(args, 1);
    this._unknowns = [];

    // check for unrecognized records
    const allTypes = [
      new DovetailDownloads(),
      new DovetailImpressions(),
      new DovetailFrequency(),
      new DynamodbData(),
      new FlightIncrements(),
      new Pingbacks(),
    ];
    records.forEach((r) => {
      if (!allTypes.some((t) => t.check(r))) {
        this._unknowns.push(r);
      }
    });
  }

  get unrecognized() {
    return this._unknowns;
  }

  insertAll() {
    if (this._types.length) {
      return Promise.all(this._types.map((t) => t.insert())).then((results) => {
        return results
          .reduce((a, b) => a.concat(b), [])
          .map((r) => {
            logger.info(`Inserted ${r.count} rows into ${r.dest}`, { dest: r.dest, rows: r.count });
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
export const BigqueryInputs = class BigqueryInputs extends Inputs {
  constructor(records) {
    super(records, new DovetailDownloads(records), new DovetailImpressions(records));
  }
};

export const DynamoInputs = class DynamoInputs extends Inputs {
  constructor(records) {
    super(records, new DynamodbData(records));
  }
};

export const PingbackInputs = class PingbackInputs extends Inputs {
  constructor(records) {
    super(records, new Pingbacks(records), new FlightIncrements(records));
  }
};

export const FrequencyInputs = class FrequencyInputs extends Inputs {
  constructor(records) {
    super(records, new DovetailFrequency(records));
  }
};
