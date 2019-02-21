'use strict';

const dynamo = require('../dynamo');

/**
 * Write records to dynamodb, waiting for CDN bytes to be downloaded
 */
module.exports = class AnteBytes {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return record.type === 'antebytes' || record.type === 'antebytespreview';
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }
    const formatted = this._records.map(r => this.format(r));
    const num = await dynamo.write(formatted);
    return [{count: num, dest: 'dynamodb'}];
  }

  format(record) {
    const formatted = JSON.parse(JSON.stringify(record));
    formatted.id = `${record.listenerSession}.${record.digest}`;
    delete formatted.listenerSession;
    delete formatted.digest;
    return formatted;
  }

};
