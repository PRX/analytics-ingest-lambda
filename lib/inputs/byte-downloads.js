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
      const formatted = this._records.map(r => {
        return this.format(r, originals[`${r.listenerEpisode}.${r.digest}`]);
      });
      const filtered = formatted.filter(r => r);

      // throw back onto kinesis
      const count = await kinesis.put(filtered);
      return [{count, dest: `kinesis:${kinesis.stream()}`}];
    }
  }

  format(record, original) {
    if (!original) {
      logger.warn(`DDB missing ${record.listenerEpisode}.${record.digest}`);
      return null;
    }
    if (!(original.type === 'antebytes' || original.type === 'antebytespreview')) {
      logger.warn(`Unknown ddb record type: ${original.type}`);
      return null;
    }

    // set record type so DovetailDownloads/Impressions will pick it up
    const postBytes = {
      type: original.type.replace('ante', 'post'),
      timestamp: record.timestamp || new Date().getTime(),
      listenerEpisode: record.listenerEpisode,
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
        logger.warn(`DDB missing segment ${record.listenerEpisode}.${record.digest}.${record.segment}`);
        return null;
      }
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
