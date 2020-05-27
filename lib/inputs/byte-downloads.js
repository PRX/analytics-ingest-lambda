'use strict';

const dynamo = require('../dynamo');
const kinesis = require('../kinesis');
const logger = require('../logger');

const MAX_RETRIES = 20;
const MAX_RETRY_MS = 300 * 1000;

/**
 * Pull records out of DynamoDB when their bytes are downloaded from the CDN
 */
module.exports = class ByteDownloads {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));

    // ddb keys to lookup (set by ante-bytes)
    this._ddbKeys = {};
    this._records.forEach(({listenerEpisode, digest}) => {
      this._ddbKeys[`${listenerEpisode}.${digest}`] = {listenerEpisode, digest};
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

      // format using the original ddb record
      const formatted = [];
      const retries = [];
      this._records.forEach(rec => {
        const key = `${rec.listenerEpisode}.${rec.digest}`
        const orig = originals[key];
        if (orig) {
          const format = this.format(rec, orig);
          if (format) {
            formatted.push(format);
          }
        } else if (this.shouldRetry(rec)) {
          retries.push(this.formatRetry(rec));
        } else {
          logger.warn(`DDB missing ${key}`);
        }
      });

      // throw back onto kinesis
      const result = [];
      if (formatted.length) {
        const count = await kinesis.put(formatted);
        result.push({count, dest: `kinesis:${kinesis.stream()}`});
      }
      if (retries.length) {
        const count = await kinesis.putRetry(retries);
        result.push({count, dest: `kinesis:${kinesis.retryStream()}`});
      }
      return result;
    }
  }

  format(record, original) {
    if (!original) {
      return null;
    }
    if (!(original.type === 'antebytes' || original.type === 'antebytespreview')) {
      logger.warn(`Unknown ddb record type: ${original.type}`);
      return null;
    }

    // set record type so DovetailDownloads/Impressions will pick it up
    const postBytes = {
      type: original.type.replace('ante', 'post'),
      timestamp: record.timestamp || Date.now(),
      listenerEpisode: record.listenerEpisode,
      listenerId: original.listenerId,
      requestUuid: original.requestUuid,
      feederPodcast: original.feederPodcast,
      feederEpisode: original.feederEpisode,
      url: original.url,
      digest: record.digest,
      remoteAgent: original.remoteAgent,
      remoteIp: original.remoteIp,
      remoteReferrer: original.remoteReferrer,
      download: null,
      impressions: [],
    };

    // format a single download/impression
    if (record.type === 'bytes') {
      return {
        ...original, // just include all original record fields
        ...postBytes,
        download: {
          isDuplicate: record.isDuplicate || false,
          cause: record.cause || null,
          adCount: original.download ? original.download.adCount : null,
        },
      };
    } else {
      const imp = original.impressions.find(i => i.segment === record.segment);
      if (imp) {
        return {
          ...postBytes,
          impressions: [{
            isDuplicate: record.isDuplicate || false,
            cause: record.cause || null,
            ...imp,
          }],
        };
      } else {
        return null;
      }
    }
  }

  // mark when/how-many-times we're retrying
  formatRetry(record) {
    if (record.retryCount) {
      record.retryCount++;
    } else {
      record.retryCount = 1;
    }
    if (!record.retryAt) {
      record.retryAt = Date.now();
    }
    return record;
  }

  // handle race condition, where dovetail-counts-lambda has us trying to read
  // from DDB before dovetail.prx.org has gotten the record written.
  shouldRetry(record) {
    if (record.retryCount && record.retryCount >= MAX_RETRIES) {
      return false;
    } else if (record.retryAt && (Date.now() - record.retryAt) > MAX_RETRY_MS) {
      return false;
    } else {
      return true;
    }
  }

  // load original records from ddb, and change 'id' back to original fields
  async lookup() {
    const originals = await dynamo.get(Object.keys(this._ddbKeys));
    const originalsMap = {};
    originals.forEach(rec => {
      if (rec && this._ddbKeys[rec.id]) {
        const remap = this._ddbKeys[rec.id];
        originalsMap[rec.id] = {...rec, ...remap};
        delete originalsMap[rec.id].id;
      }
    });
    return originalsMap;
  }

};
