'use strict';

const uuid = require('uuid');
const crypto = require('crypto');
const timestamp = require('../timestamp');
const assayer = require('../assayer');
const bigquery = require('../bigquery');

/**
 * Send dovetail impressions to bigquery
 */
module.exports = class DovetailImpressions {
  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    if (record.type === 'combined' || record.type === 'postbytes') {
      return (record.impressions || []).length > 0;
    } else {
      return false;
    }
  }

  tableName(record) {
    return 'dt_impressions';
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // format records and organize by table name
    const formatted = {};
    await this.eachImpression(async (rec, imp) => {
      const name = this.tableName(rec);
      const formattedRec = await this.format(rec, imp);
      if (formatted[name]) {
        formatted[name].push(formattedRec);
      } else {
        formatted[name] = [formattedRec];
      }
    });
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

  async format(record, imp) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0);
    const info = await assayer.testImpression(record, imp, true);
    let out = {
      insertId: this.md5InsertId(`${record.listenerEpisode}/${epoch}`, imp),
      json: {
        timestamp: epoch,
        request_uuid: record.requestUuid || uuid.v4(),
        feeder_podcast: record.feederPodcast,
        feeder_feed: record.feederFeed || null,
        feeder_episode: record.feederEpisode,
        digest: record.digest,
        is_confirmed: !!record.confirmed,
        segment: imp.segment,
        ad_id: imp.adId,
        campaign_id: imp.campaignId,
        creative_id: imp.creativeId,
        flight_id: imp.flightId,
        target_path: imp.targetPath || null,
        zone_name: imp.zoneName || null,
        placements_key: imp.placementsKey || null,
        is_duplicate: info.isDuplicate,
        cause: info.cause,
        listener_id: record.listenerId,
        agent_name_id: info.agent.name,
        agent_type_id: info.agent.type,
        agent_os_id: info.agent.os,
        city_geoname_id: info.geo.city,
        country_geoname_id: info.geo.country,
      },
    };

    if (imp.vast) {
      out.json.vast_advertiser = imp.vast.advertiser;

      if (imp.vast.ad) {
        out.json.vast_ad_id = imp.vast.ad.id;
      }

      if (imp.vast.creative) {
        out.json.vast_creative_id = imp.vast.creative.id;
      }

      if (imp.vast.pricing) {
        out.json.vast_price_value = parseFloat(imp.vast.pricing.value);
        out.json.vast_price_currency = imp.vast.pricing.currency;
        out.json.vast_price_model = imp.vast.pricing.model;
      }
    }
    return out;
  }

  md5InsertId(unique, impression) {
    const parts = [
      unique,
      impression.adId,
      impression.campaignId,
      impression.creativeId,
      impression.flightId,
    ].join('-');
    return crypto.createHash('md5').update(parts).digest('hex');
  }

  async eachImpression(handler) {
    await Promise.all(
      this._records.map(async rec => {
        await Promise.all(
          rec.impressions.map(async imp => {
            await handler(rec, imp);
          }),
        );
      }),
    );
  }
};
