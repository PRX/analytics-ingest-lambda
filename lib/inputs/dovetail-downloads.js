'use strict';

const uuidv4 = require('uuid/v4');
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
    if (record.type === 'combined' || record.type === 'postbytes' || record.type === 'postbytespreview') {
      return !!record.download;
    } else {
      return false;
    }
  }

  tableName(record) {
    if (record.type === 'postbytespreview') {
      return 'dt_downloads_preview';
    } else {
      return 'dt_downloads';
    }
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // format records and organize by table name
    const formatted = {};
    await Promise.all(this._records.map(async (rec) => {
      const name = this.tableName(rec);
      const formattedRec = await this.format(rec);
      if (formatted[name]) {
        formatted[name].push(formattedRec);
      } else {
        formatted[name] = [formattedRec];
      }
    }));
    const tableNames = Object.keys(formatted);

    // insert in parallel
    return await Promise.all(tableNames.map(async (tbl) => {
      const num = await bigquery.insert(tbl, formatted[tbl]);
      return {count: num, dest: tbl};
    }));
  }

  async format(record) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);
    const listenerSession = timestamp.toDigest(record.listenerEpisode, epoch);
    const lookups = [lookip.look(record.remoteIp), lookagent.look(record.remoteAgent)];
    const [geo, agent] = await Promise.all(lookups);

    return {
      insertId: `${listenerSession}/${epoch}`,
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
        is_bytes:           this.isBytes(record),
        url:                record.url,
        // listener data
        listener_id:        record.listenerId,
        listener_episode:   record.listenerEpisode,
        listener_session:   listenerSession,
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

  isBytes(record) {
    return record.type === 'postbytes' || record.type === 'postbytespreview';
  }

};
