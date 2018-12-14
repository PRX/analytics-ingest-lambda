'use strict';

const uuidv4 = require('uuid/v4');
const timestamp = require('../timestamp');
const lookip = require('../lookip');
const lookagent = require('../lookagent');
const bigquery = require('../bigquery');
const TABLE_NAME = 'dt_downloads';

/**
 * Send dovetail downloads to bigquery
 */
module.exports = class DovetailDownloads {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return record.type === 'combined' && !record.bytesCompliance && !!record.download;
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }
    const formatted = await Promise.all(this._records.map(r => this.format(r)));
    const num = await bigquery.insert(TABLE_NAME, formatted);
    return [{count: num, dest: TABLE_NAME}];
  }

  async format(record) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);
    const lookups = [lookip.look(record.remoteIp), lookagent.look(record.remoteAgent)];
    const [geo, agent] = await Promise.all(lookups);

    return {
      insertId: `${record.listenerEpisode}/${epoch}`,
      json: {
        timestamp:          epoch,
        request_uuid:       record.requestUuid || uuidv4(),
        // redirect data
        feeder_podcast:     record.feederPodcast,
        feeder_episode:     record.feederEpisode,
        digest:             record.digest,
        ad_count:           record.download.adCount,
        is_duplicate:       !!record.download.isDuplicate,
        cause:              record.download.cause,
        is_confirmed:       !!record.confirmed,
        is_bytes:           false,
        url:                record.url,
        // listener data
        listener_id:        record.listenerId,
        listener_episode:   record.listenerEpisode,
        listener_session:   record.listenerSession,
        // request data
        remote_referrer:    record.remoteReferrer,
        remote_agent:       record.remoteAgent,
        remote_ip:          geo.masked,
        // derived data
        agent_name_id:      agent.name,
        agent_type_id:      agent.type,
        agent_os_id:        agent.os,
        city_geoname_id:    geo.city,
        country_geoname_id: geo.country,
        postal_code:        geo.postal,
        latitude:           geo.latitude,
        longitude:          geo.longitude
      }
    };
  }

}
