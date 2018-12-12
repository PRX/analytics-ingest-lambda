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
    return record.type === 'combined' && record.impressions && record.impressions.length > 0;
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
        insertId: this.md5InsertId(`${record.listenerEpisode}-${epoch}`, imp),
        json: {
          timestamp:        epoch,
          feeder_podcast:   record.feederPodcast,
          feeder_episode:   record.feederEpisode,
          listener_id:      record.listenerId,      // TODO: new
          listener_episode: record.listenerEpisode, // TODO: new
          listener_session: record.listenerSession, // TODO: new
          confirmed:        !!record.confirmed,     // TODO: new
          segment:          imp.segment,            // TODO: new
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
