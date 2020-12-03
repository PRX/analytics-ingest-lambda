'use strict';

const uuid = require('uuid');
const crypto = require('crypto');
const timestamp = require('../timestamp');
const assayer = require('../assayer');
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
    await this.eachImpression(async (rec, imp) => {
      const name = this.tableName(rec);
      const formattedRec = await this.format(rec, imp);
      if (formatted[name]) {
        formatted[name].push(formattedRec);
      } else {
        formatted[name] = [formattedRec];
      }
    });
    const tableNames = Object.keys(formatted);

    // insert in parallel
    const ds = process.env.BQ_DATASET || '';
    return await Promise.all(tableNames.map(async (tbl) => {
      const num = await bigquery.insert(ds, tbl, formatted[tbl]);
      return {count: num, dest: tbl};
    }));
  }

  async format(record, imp) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);
    const listenerSession = timestamp.toDigest(record.listenerEpisode, epoch);
    const info = await assayer.testImpression(record, imp);

    return {
      insertId: this.md5InsertId(`${listenerSession}/${epoch}`, imp),
      json: {
        timestamp:        epoch,
        request_uuid:     record.requestUuid || uuid.v4(),
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
        target_path:      imp.targetPath || null,
        zone_name:        imp.zoneName || null,
        placements_key:   imp.placementsKey || null,
        is_duplicate:     info.isDuplicate,
        cause:            info.cause
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

  async eachImpression(handler) {
    await Promise.all(this._records.map(async (rec) => {
      await Promise.all(rec.impressions.map(async (imp) => {
        await handler(rec, imp);
      }));
    }));
  }

};
