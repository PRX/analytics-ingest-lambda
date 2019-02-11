'use strict';

const dynamo = require('../dynamo');

/**
 * Pull records out of DynamoDB when their bytes are downloaded from the CDN
 */
module.exports = class DovetailBytes {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
    this._downloads = this._records.filter(r => r.type === 'bytes').map(r => {
      return `${r.listenerSession}-${r.digest}`;
    });
    this._impressions = this._records.filter(r => r.type === 'segmentbytes').map(r => {
      return `${r.listenerSession}-${r.digest}-${r.segment}`;
    });
  }

  check(record) {
    return record.type === 'bytes' || record.type === 'segmentbytes';
  }

  async insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    } else {
      const keys = this._records.map(r => `${r.listenerSession}-${r.digest}`);
      const uniqueKeys = keys.filter((key, idx) => keys.indexOf(key) === idx);

      // load original records from ddb, and filter to bytes/segmentbytes
      // TODO: these could have been previously processed - really need some
      // way to mark which ddb downloads/impressions we've already processed
      const originals = await dynamo.get(uniqueKeys);
      const filtered = this.filter(originals);

      // throw back onto kinesis
      console.log('TODO: kinesis', filtered);

      return [{count: filtered.length, dest: 'kinesis'}];
    }
  }

  filter(recs = []) {
    recs.forEach(rec => {
      const lsd = `${rec.listenerSession}-${rec.digest}`;
      if (!this._downloads.includes(lsd)) {
        rec.download = null;
      }
      rec.impressions = (rec.impressions || []).filter(impression => {
        return this._impressions.includes(`${lsd}-${impression.segment}`);
      });

      // bytes downloaded are never duplicate
      if (rec.download) {
        rec.download.isDuplicate = false;
        rec.download.cause = null;
      }
      rec.impressions.forEach(imp => {
        imp.isDuplicate = false;
        imp.cause = null;
      });

      // only run pingbacks in compliance mode
      if (!rec.bytesCompliance) {
        rec.impressions.forEach(imp => imp.pings = []);
      }

      // change the record type
      rec.type = 'combinedbytes';
    });

    // only return preview/compliance records that have a download/impression
    return recs.filter(r => r.download || r.impressions.length).filter(r => r.bytesPreview || r.bytesCompliance);
  }

};
