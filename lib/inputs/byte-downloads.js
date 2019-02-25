'use strict';

const dynamo = require('../dynamo');
const kinesis = require('../kinesis');
const logger = require('../logger');

/**
 * Pull records out of DynamoDB when their bytes are downloaded from the CDN
 */
module.exports = class ByteDownloads {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));

    // unique keys for downloads/impressions
    this._allKeys = {};
    this._downloadKeys = {};
    this._impressionKeys = {};
    this._records.forEach(r => {
      this._allKeys[`${r.listenerSession}.${r.digest}`] = {
        listenerSession: r.listenerSession,
        digest: r.digest,
      };
      if (r.type === 'bytes') {
        this._downloadKeys[`${r.listenerSession}.${r.digest}`] = true;
      } else {
        this._impressionKeys[`${r.listenerSession}.${r.digest}.${r.segment}`] = true;
      }
    });
  }

  check(record) {
    return record.type === 'bytes' || record.type === 'segmentbytes';
  }

  async insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    } else {
      const originals = await this.lookup();
      const filtered = this.filter(originals);

      // throw back onto kinesis
      const count = await kinesis.put(filtered);
      return [{count, dest: `kinesis:${kinesis.stream()}`}];
    }
  }

  filter(recs = []) {
    const nonnulls = recs.filter(r => r);
    nonnulls.forEach(rec => {
      const lsd = `${rec.listenerSession}.${rec.digest}`;

      // remove downloads/impressions that weren't flagged yet
      rec.download = this._downloadKeys[lsd] ? rec.download : null;
      rec.impressions = (rec.impressions || []).filter(i => this._impressionKeys[`${lsd}.${i.segment}`]);

      // duplicates/reasons deprecated for byte downloads
      if (rec.download) {
        delete rec.download.isDuplicate;
        delete rec.download.reason;
      }
      rec.impressions.forEach(i => {
        delete i.isDuplicate;
        delete i.reason;
      });

      // set record type so DovetailDownloads/Impressions will pick it up
      if (rec.type === 'antebytes') {
        rec.type = 'postbytes';
      } else if (rec.type === 'antebytespreview') {
        rec.type = 'postbytespreview';
      } else {
        logger.warn(`Unknown ddb record type: ${rec.type}`);
        rec.type = 'unknown';
      }
    });

    // only return records that actually have a download/impression
    return nonnulls.filter(r => r.download || r.impressions.length);
  }

  // load original records from ddb, and change 'id' back to original fields.
  // TODO: these could have been previously processed - really need some
  // way to mark which ddb downloads/impressions we've already processed
  async lookup() {
    const originals = await dynamo.get(Object.keys(this._allKeys));

    // change `id` back to `listenerSession + digest`
    return originals.map(rec => {
      if (rec && this._allKeys[rec.id]) {
        const remap = this._allKeys[rec.id];
        delete rec.id;
        return {...rec, ...remap};
      } else {
        return rec;
      }
    });
  }

};
