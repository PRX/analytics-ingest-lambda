'use strict';

const timestamp = require('../timestamp');
const lookip = require('../lookip');

/**
 * Does this look like a dovetail download record?
 */
exports.check = (record) => {
  return record.type === 'download';
};

/**
 * Get the table/partition to insert into
 */
exports.table = (record) => {
  let tbl = process.env.BQ_DOWNLOADS_TABLE;
  let day = timestamp.toDateString(record.timestamp || 0);
  return tbl + '$' + day;
};

/**
 * Convert to bigquery table format
 */
exports.format = (record) => {
  return lookip.look(record.remoteIp).then(look => {
    return {
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
        city_id:        look.city,
        country_id:     look.country
      }
    };
  });
};
