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

    // id used as ddb key - remove redundant keys
    formatted.id = `${record.listenerEpisode}.${record.digest}`;
    delete formatted.listenerEpisode;
    delete formatted.digest;

    // duplicates/causes deprecated for byte downloads
    if (formatted.download) {
      delete formatted.download.isDuplicate;
      delete formatted.download.cause;
    }
    (formatted.impressions || []).forEach(i => {
      delete i.isDuplicate;
      delete i.cause;
    });

    return formatted;
  }

};
