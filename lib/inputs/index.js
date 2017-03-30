'use strict';

const dovetailDownload = require('./dovetail-download');
const dovetailImpression = require('./dovetail-impression');
const TYPES = [dovetailDownload, dovetailImpression];

/**
 * Delegate input parsers
 */
module.exports = class Inputs {

  constructor(records) {
    this._records = {};
    this._unknowns = [];

    // group records by type
    if (records) {
      records.forEach(r => {
        let inputType = TYPES.find(t => t.check(r));
        if (inputType) {
          let table = inputType.table(r);
          if (this._records[table]) {
            this._records[table].push(inputType.format(r));
          } else {
            this._records[table] = [inputType.format(r)];
          }
        } else {
          this._unknowns.push(r);
        }
      });
    }
  }

  get tables() {
    return Object.keys(this._records);
  }

  get unrecognized() {
    return this._unknowns;
  }

  get outputs() {
    return Object.keys(this._records).map(tableName => {
      return [tableName, this._records[tableName]];
    });
  }

}
