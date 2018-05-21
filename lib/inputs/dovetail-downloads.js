'use strict';

const timestamp = require('../timestamp');
const lookip = require('../lookip');
const lookagent = require('../lookagent');
const bigquery = require('../bigquery');

/**
 * Send dovetail downloads to bigquery
 */
module.exports = class DovetailDownloads {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return record.type === 'download';
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
          insertId: record.requestUuid,
          json: {
            digest:         record.digest,
            program:        record.program,
            path:           record.path,
            feeder_podcast: record.feederPodcast,
            feeder_episode: record.feederEpisode,
            remote_agent:   record.remoteAgent,
            remote_ip:      record.remoteIp,
            timestamp:      timestamp.toEpochSeconds(record.timestamp || 0),
            request_uuid:   record.requestUuid,
            ad_count:       record.adCount,
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
    let tbl = 'dt_downloads';
    let day = timestamp.toDateString(record.timestamp || 0);
    return tbl + '$' + day;
  }

}
