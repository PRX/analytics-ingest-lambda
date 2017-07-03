'use strict';

const dovetailDownload = require('./dovetail-download');
const dovetailImpression = require('./dovetail-impression');
const TYPES = [dovetailDownload, dovetailImpression];

/**
 * Delegate input parsers
 */
module.exports = class Inputs {

  constructor(records) {
    this._tables = {};
    this._unknowns = [];

    // group records by type
    if (records) {
      records.forEach(r => {
        let inputType = TYPES.find(t => t.check(r));
        if (inputType) {
          let table = inputType.table(r);
          if (this._tables[table]) {
            this._tables[table].push(inputType.format(r));
          } else {
            this._tables[table] = [inputType.format(r)];
          }
        } else {
          this._unknowns.push(r);
        }
      });
    }
  }

  get tables() {
    return Object.keys(this._tables);
  }

  get unrecognized() {
    return this._unknowns;
  }

  formatRecordsForTable(tbl) {
    return Promise.all(this._tables[tbl]);
  }

  formatRecords() {
    return Promise.all(Object.keys(this._tables).map(t => {
      return this.formatRecordsForTable(t).then(formatted => [t, formatted]);
    }));
  }

}
