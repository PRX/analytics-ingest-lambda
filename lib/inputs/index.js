'use strict';

const dovetail = require('./dovetail');

/**
 * Delegate input parsers
 */
module.exports = class Inputs {

  constructor(records = []) {
    this._types = {};
    this._records = {};
    this._unknowns = [];

    // map table names to input types
    this._types[process.env.BQ_DOVETAIL_TABLE] = dovetail;
    this._records[process.env.BQ_DOVETAIL_TABLE] = [];

    // group records by type
    records.forEach(r => {
      let type = this.types.find(t => this._types[t].check(r));
      if (type) {
        this._records[type].push(r);
      } else {
        this._unknowns.push(r);
      }
    });
  }

  get types() {
    return Object.keys(this._types);
  }

  unrecognized() {
    return this._unknowns;
  }

  outputs(type) {
    if (!this._types[type]) {
      throw new Error(`Unknown type: ${type}`);
    }
    return this._records[type].map(r => this._types[type].toOutput(r));
  }

  outputsByType() {
    return this.types.map(type => {
      return [type, this.outputs(type)];
    });
  }

}
