'use strict';

const crypto = require('crypto');
const timestamp = require('../timestamp');
const lookip = require('../lookip');
const lookagent = require('../lookagent');
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
    let lookups = [lookip.look(record.remoteIp), lookagent.look(record.remoteAgent)];
    return Promise.all(lookups).then(([geo, agent]) => {
      return {
        table: this.table(record),
        record: {
          // TODO: this ASSUMES any ad is only found once per arrangement!!!
          insertId: this.md5(record),
          json: {
            protocol:       record.protocol,
            host:           record.host,
            query:          record.query,
            digest:         record.digest,
            program:        record.program,
            path:           record.path,
            feeder_podcast: record.feederPodcast,
            feeder_episode: record.feederEpisode,
            remote_agent:   record.remoteAgent,
            remote_ip:      record.remoteIp,
            timestamp:      timestamp.toEpochSeconds(record.timestamp || 0),
            request_uuid:   record.requestUuid,
            ad_id:          record.adId,
            campaign_id:    record.campaignId,
            creative_id:    record.creativeId,
            flight_id:      record.flightId,
            is_duplicate:   record.isDuplicate,
            cause:          record.cause,
            city_id:        geo.city,
            country_id:     geo.country,
            agent_name_id:  agent.name,
            agent_type_id:  agent.type,
            agent_os_id:    agent.os
          }
        }
      };
    });
  }

  table(record) {
    let tbl = process.env.BQ_IMPRESSIONS_TABLE;
    let day = timestamp.toDateString(record.timestamp || 0);
    return tbl + '$' + day;
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
