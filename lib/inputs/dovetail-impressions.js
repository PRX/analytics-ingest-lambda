'use strict';

const crypto = require('crypto');
const timestamp = require('../timestamp');
const bigquery = require('../bigquery');
const TABLE_NAME = 'dt_impressions';

/**
 * Send dovetail impressions to bigquery
 */
module.exports = class DovetailImpressions {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return (record.type === 'combined' && record.impressions && record.impressions.length > 0)
         || record.type === 'impression';
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }
    const formatted = await Promise.all(this._records.map(r => this.format(r)));

    // flatten formatted records - combined records are arrays-of-arrays
    const flattened = [].concat.apply([], formatted);
    const num = await bigquery.insert(TABLE_NAME, flattened);
    return [{count: num, dest: TABLE_NAME}];
  }

  async format(record) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);

    // TODO: remove legacy 'impression' type
    if (record.type === 'combined') {
      return this.formatCombined(record, epoch);
    } else {
      return this.formatLegacy(record, epoch);
    }
  }

  formatCombined(record, epoch) {
    return record.impressions.map(imp => {
      return {
        insertId: this.md5InsertId(`${record.listenerEpisode}-${epoch}`, imp),
        json: {
          timestamp:        epoch,
          feeder_podcast:   record.feederPodcast,
          feeder_episode:   record.feederEpisode,
          listener_id:      record.listenerId,      // TODO: new
          listener_episode: record.listenerEpisode, // TODO: new
          listener_session: record.listenerSession, // TODO: new
          confirmed:        record.confirmed,       // TODO: new
          segment:          imp.segment,            // TODO: new
          ad_id:            imp.adId,
          campaign_id:      imp.campaignId,
          creative_id:      imp.creativeId,
          flight_id:        imp.flightId,
          is_duplicate:     imp.isDuplicate,
          cause:            imp.cause
        }
      };
    });
  }

  formatLegacy(record, epoch) {
    return {
      insertId: this.md5InsertId(record.requestUuid, record),
      json: {
        timestamp:      epoch,
        request_uuid:   record.requestUuid,
        feeder_podcast: record.feederPodcast,
        feeder_episode: record.feederEpisode,
        is_duplicate:   record.isDuplicate,
        cause:          record.cause,
        ad_id:          record.adId,
        campaign_id:    record.campaignId,
        creative_id:    record.creativeId,
        flight_id:      record.flightId
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

}
