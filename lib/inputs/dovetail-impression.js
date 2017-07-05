'use strict';

const crypto = require('crypto');
const timestamp = require('../timestamp');
const lookip = require('../lookip');

// unique insert id ... req uuid + ad info
// TODO: this ASSUMES any ad is only found once per arrangement!!!
function md5(record) {
  let data = [
    record.requestUuid,
    record.adId,
    record.campaignId,
    record.creativeId,
    record.flightId
  ].join('-');
  return crypto.createHash('md5').update(data).digest('hex');
}

/**
 * Does this look like a dovetail impression record?
 */
exports.check = (record) => {
  return (record.type === undefined && !!record.adId) || record.type === 'impression';
};

/**
 * Get the table/partition to insert into
 */
exports.table = (record) => {
  let tbl = process.env.BQ_IMPRESSIONS_TABLE;
  let day = timestamp.toDateString(record.timestamp || 0);
  return tbl + '$' + day;
};

/**
 * Convert to bigquery table format
 *
 * NOTE: there's nothing unique about an impression, since a single request
 * might serve up the same ad more than once. So just generate an insert id.
 */
exports.format = (record) => {
  return lookip.look(record.remoteIp).then(look => {
    return {
      insertId: md5(record),
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
        ad_id:          record.adId,
        campaign_id:    record.campaignId,
        creative_id:    record.creativeId,
        flight_id:      record.flightId,
        is_duplicate:   record.isDuplicate,
        cause:          record.cause,
        city_id:        look.city,
        country_id:     look.country
      }
    };
  });
};
