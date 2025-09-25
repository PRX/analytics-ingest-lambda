import { createHash } from "node:crypto";
import log from "lambda-log";
import { v4 } from "uuid";
import * as bigquery from "./lib/bigquery";
import { decodeRecords } from "./lib/decoder";
import { clean, mask } from "./lib/iputil";
import { toEpochSeconds } from "./lib/timestamp";

/**
 * Insert counted Kinesis records into BigQuery
 */
export const handler = async (event) => {
  const records = await decodeRecords(event);
  const downs = records.filter((r) => r.type === "postbytes" && !!r.download);
  const imps = records
    .filter((r) => r.type === "postbytes")
    .flatMap((r) => (r.impressions || []).map((i) => [r, i]));

  const info = { records: records.length, downloads: downs.length, impressions: imps.length };
  log.info("Starting BigQuery", info);

  // ugh, needed for testing, because you can't mock ES module exports
  const client = event.bigqueryClient || (await bigquery.client());

  const downRows = downs.map((r) => formatDownload(r));
  const downCount = await bigquery.insert({ client, table: "dt_downloads", rows: downRows });

  const impRows = imps.map((r, i) => formatImpression(r, i));
  const impCount = await bigquery.insert({ client, table: "dt_impressions", rows: impRows });

  const info2 = { records: records.length, downloads: downCount, impressions: impCount };
  log.info("Finished BigQuery", info2);
};

/**
 * Common data in dt_downloads/dt_impressions
 */
export const format = (rec) => {
  return {
    timestamp: toEpochSeconds(rec.timestamp || 0),
    request_uuid: rec.requestUuid || v4(),
    feeder_podcast: rec.feederPodcast,
    feeder_feed: rec.feederFeed || null,
    feeder_episode: rec.feederEpisode,
    digest: rec.digest,
    is_confirmed: !!rec.confirmed,
    listener_id: rec.listenerId,
    agent_name_id: rec.agentName,
    agent_type_id: rec.agentType,
    agent_os_id: rec.agentOs,
    city_geoname_id: rec.city,
    country_geoname_id: rec.country,
    postal_code: rec.postalCode,
  };
};

/**
 * Raw insert for dt_downloads (including insert ids for BQ de-duping)
 */
export const formatDownload = (rec) => {
  const row = format(rec);
  return {
    insertId: `${rec.listenerEpisode}/${row.timestamp}`,
    json: {
      ...row,
      is_duplicate: !!rec.download?.isDuplicate,
      cause: rec.download?.cause,
      ad_count: rec.download?.adCount,
      url: rec.url,
      listener_episode: rec.listenerEpisode,
      remote_referrer: rec.remoteReferrer,
      remote_agent: rec.remoteAgent,
      remote_ip: mask(clean(rec.remoteIp)),
      zones_filled_pre: rec.filled?.paid?.[0],
      zones_filled_mid: rec.filled?.paid?.[1],
      zones_filled_post: rec.filled?.paid?.[2],
      zones_filled_house_pre: rec.filled?.house?.[0],
      zones_filled_house_mid: rec.filled?.house?.[1],
      zones_filled_house_post: rec.filled?.house?.[2],
      zones_unfilled_pre: rec.unfilled?.paid?.[0],
      zones_unfilled_mid: rec.unfilled?.paid?.[1],
      zones_unfilled_post: rec.unfilled?.paid?.[2],
      zones_unfilled_house_pre: rec.unfilled?.house?.[0],
      zones_unfilled_house_mid: rec.unfilled?.house?.[1],
      zones_unfilled_house_post: rec.unfilled?.house?.[2],
    },
  };
};

/**
 * Raw insert for dt_impressions (including insert ids for BQ de-duping)
 */
export const formatImpression = ([rec, imp]) => {
  const row = format(rec);

  // unique insert id for this ad within the download
  const parts = [rec.listenerEpisode, row.timestamp];
  parts.push(imp.adId, imp.campaignId, imp.creativeId, imp.flightId);
  const id = createHash("md5").update(parts.join("-")).digest("hex");

  return {
    insertId: id,
    json: {
      ...row,
      is_duplicate: !!imp.isDuplicate,
      cause: imp.cause,
      segment: imp.segment,
      ad_id: imp.adId,
      campaign_id: imp.campaignId,
      creative_id: imp.creativeId,
      flight_id: imp.flightId,
      target_path: imp.targetPath || null,
      zone_name: imp.zoneName || null,
      placements_key: imp.placementsKey || null,
      vast_advertiser: imp.vast?.advertiser,
      vast_ad_id: imp.vast?.ad?.id,
      vast_creative_id: imp.vast?.creative?.id,
      vast_price_value: parseFloat(imp.vast?.pricing?.value) || null,
      vast_price_currency: imp.vast?.pricing?.currency,
      vast_price_model: imp.vast?.pricing?.model,
    },
  };
};
