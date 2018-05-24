'use strict';

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
    return (record.type === undefined && !!record.adId) || record.type === 'impression';
  }

  insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    } else {
      return Promise.all(this._records.map(r => this.format(r))).then(formats => {
        let tables = {};
        formats.forEach(f => tables[f.table] = (tables[f.table] || []).concat(f.record));

        // run per-table inserts in parallel
        return Promise.all(Object.keys(tables).map(t => {
          return bigquery.insert(t, tables[t]).then(num => {
            return {count: num, dest: t};
          });
        }));
      });
    }
  }

  format(record) {
    return Promise.resolve({
      table: 'dt_impressions',
      record: {
        // TODO: this ASSUMES any ad is only found once per arrangement!!!
        insertId: this.md5(record),
        json: {
          timestamp:      timestamp.toEpochSeconds(record.timestamp || 0),
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
      }
    });
  }

  md5(record) {
    let data = [
      record.requestUuid,
      record.adId,
      record.campaignId,
      record.creativeId,
      record.flightId
    ].join('-');
    return crypto.createHash('md5').update(data).digest('hex');
  }

}
