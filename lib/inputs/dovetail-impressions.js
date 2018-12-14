'use strict';

const uuidv4 = require('uuid/v4');
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
    return record.type === 'combined' && !record.bytesCompliance && record.impressions && record.impressions.length > 0;
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
    return record.impressions.map(imp => {
      return {
        insertId: this.md5InsertId(`${record.listenerSession}-${epoch}`, imp),
        json: {
          timestamp:        epoch,
          request_uuid:     record.requestUuid || uuidv4(),
          feeder_podcast:   record.feederPodcast,
          feeder_episode:   record.feederEpisode,
          digest:           record.digest,
          listener_session: record.listenerSession,
          is_confirmed:     !!record.confirmed,
          is_bytes:         false,
          segment:          imp.segment,
          ad_id:            imp.adId,
          campaign_id:      imp.campaignId,
          creative_id:      imp.creativeId,
          flight_id:        imp.flightId,
          is_duplicate:     !!imp.isDuplicate,
          cause:            imp.cause
        }
      };
    });
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
