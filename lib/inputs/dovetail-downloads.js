'use strict';

const uuid = require('uuid');
const timestamp = require('../timestamp');
const bigquery = require('../bigquery');
const iputil = require('../iputil');

/**
 * Send dovetail downloads to bigquery
 */
module.exports = class DovetailDownloads {
  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return record.type === 'postbytes' && !!record.download;
  }

  tableName(record) {
    return 'dt_downloads';
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // format records and organize by table name
    const formatted = {};
    await Promise.all(
      this._records.map(async rec => {
        const name = this.tableName(rec);
        const formattedRec = await this.format(rec);
        if (formatted[name]) {
          formatted[name].push(formattedRec);
        } else {
          formatted[name] = [formattedRec];
        }
      }),
    );
    const tableNames = Object.keys(formatted);

    // insert in parallel
    const ds = process.env.BQ_DATASET || '';
    return await Promise.all(
      tableNames.map(async tbl => {
        const num = await bigquery.insert(ds, tbl, formatted[tbl]);
        return { count: num, dest: tbl };
      }),
    );
  }

  async format(record) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);

    return {
      insertId: `${record.listenerEpisode}/${epoch}`,
      json: {
        timestamp: epoch,
        request_uuid: record.requestUuid || uuid.v4(),
        // redirect data
        feeder_podcast: record.feederPodcast,
        feeder_feed: record.feederFeed || null,
        feeder_episode: record.feederEpisode,
        digest: record.digest,
        ad_count: record.download.adCount,
        is_duplicate: !!record.download.isDuplicate,
        cause: record.download.cause,
        is_confirmed: !!record.confirmed,
        url: record.url,
        // listener data
        listener_id: record.listenerId,
        listener_episode: record.listenerEpisode,
        // request data
        remote_referrer: record.remoteReferrer,
        remote_agent: record.remoteAgent,
        remote_ip: iputil.mask(iputil.clean(record.remoteIp)),
        // derived data
        agent_name_id: record.agentName,
        agent_type_id: record.agentType,
        agent_os_id: record.agentOs,
        city_geoname_id: record.city,
        country_geoname_id: record.country,
        postal_code: record.postalCode,
        // fill rates
        zones_filled_pre: record.filled?.paid?.[0],
        zones_filled_mid: record.filled?.paid?.[1],
        zones_filled_post: record.filled?.paid?.[2],
        zones_filled_house_pre: record.filled?.house?.[0],
        zones_filled_house_mid: record.filled?.house?.[1],
        zones_filled_house_post: record.filled?.house?.[2],
        zones_unfilled_pre: record.unfilled?.paid?.[0],
        zones_unfilled_mid: record.unfilled?.paid?.[1],
        zones_unfilled_post: record.unfilled?.paid?.[2],
        zones_unfilled_house_pre: record.unfilled?.house?.[0],
        zones_unfilled_house_mid: record.unfilled?.house?.[1],
        zones_unfilled_house_post: record.unfilled?.house?.[2],
      },
    };
  }
};
