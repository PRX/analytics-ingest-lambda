'use strict';

const uuid = require('uuid');
const timestamp = require('../timestamp');
const assayer = require('../assayer');
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
    const ds = process.env.BQ_DATASET || '';
    return await Promise.all(tableNames.map(async (tbl) => {
      const num = await bigquery.insert(ds, tbl, formatted[tbl]);
      return {count: num, dest: tbl};
    }));
  }

  async format(record) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);
    const listenerSession = timestamp.toDigest(record.listenerEpisode, epoch);
    const info = await assayer.test(record, true);

    return {
      insertId: `${listenerSession}/${epoch}`,
      json: {
        timestamp:          epoch,
        request_uuid:       record.requestUuid || uuid.v4(),
        // redirect data
        feeder_podcast:     record.feederPodcast,
        feeder_episode:     record.feederEpisode,
        digest:             record.digest,
        ad_count:           record.download.adCount,
        is_duplicate:       info.isDuplicate,
        cause:              info.cause,
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
        remote_ip:          info.geo.masked,
        // derived data
        agent_name_id:      info.agent.name,
        agent_type_id:      info.agent.type,
        agent_os_id:        info.agent.os,
        city_geoname_id:    info.geo.city,
        country_geoname_id: info.geo.country,
        postal_code:        info.geo.postal,
        latitude:           info.geo.latitude,
        longitude:          info.geo.longitude
      }
    };
  }

  isBytes(record) {
    return record.type === 'postbytes' || record.type === 'postbytespreview';
  }

};
