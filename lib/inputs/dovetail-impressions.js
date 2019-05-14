'use strict';

const uuidv4 = require('uuid/v4');
const crypto = require('crypto');
const timestamp = require('../timestamp');
const bigquery = require('../bigquery');

/**
 * Send dovetail impressions to bigquery
 */
module.exports = class DovetailImpressions {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    if (record.type === 'combined' || record.type === 'postbytes' || record.type === 'postbytespreview') {
      return (record.impressions || []).length > 0;
    } else {
      return false;
    }
  }

  tableName(record) {
    if (record.type === 'postbytespreview') {
      return 'dt_impressions_preview';
    } else {
      return 'dt_impressions';
    }
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // format records and organize by table name
    const formatted = {};
    this._records.forEach(rec => {
      const name = this.tableName(rec);
      rec.impressions.forEach(imp => {
        if (formatted[name]) {
          formatted[name].push(this.format(rec, imp));
        } else {
          formatted[name] = [this.format(rec, imp)];
        }
      });
    });
    const tableNames = Object.keys(formatted);

    // insert in parallel
    const ds = process.env.BQ_DATASET || '';
    return await Promise.all(tableNames.map(async (tbl) => {
      const num = await bigquery.insert(ds, tbl, formatted[tbl]);
      return {count: num, dest: tbl};
    }));
  }

  format(record, imp) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);
    const listenerSession = timestamp.toDigest(record.listenerEpisode, epoch);
    return {
      insertId: this.md5InsertId(`${listenerSession}/${epoch}`, imp),
      json: {
        timestamp:        epoch,
        request_uuid:     record.requestUuid || uuidv4(),
        feeder_podcast:   record.feederPodcast,
        feeder_episode:   record.feederEpisode,
        digest:           record.digest,
        listener_session: listenerSession,
        is_confirmed:     !!record.confirmed,
        is_bytes:         this.isBytes(record),
        segment:          imp.segment,
        ad_id:            imp.adId,
        campaign_id:      imp.campaignId,
        creative_id:      imp.creativeId,
        flight_id:        imp.flightId,
        is_duplicate:     !!imp.isDuplicate,
        cause:            imp.cause
      }
    };
  }

  md5InsertId(unique, impression) {
    const parts = [
      unique,
      impression.adId,
      impression.campaignId,
      impression.creativeId,
      impression.flightId
    ].join('-');
    return crypto.createHash('md5').update(parts).digest('hex');
  }

  isBytes(record) {
    return record.type === 'postbytes' || record.type === 'postbytespreview';
  }

};
