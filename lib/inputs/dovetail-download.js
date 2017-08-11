'use strict';

const timestamp = require('../timestamp');
const lookip = require('../lookip');
const lookagent = require('../lookagent');

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
  let lookups = [lookip.look(record.remoteIp), lookagent.look(record.remoteAgent)];
  return Promise.all(lookups).then(([geo, agent]) => {
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
        city_id:        geo.city,
        country_id:     geo.country,
        agent_name_id:  agent.name,
        agent_type_id:  agent.type,
        agent_os_id:    agent.os
      }
    };
  });
};
