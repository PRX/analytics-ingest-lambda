'use strict';

const useragent = require('./assays/useragent');
const domainthreat = require('./assays/domainthreat');
const datacenter = require('./assays/datacenter');

/**
 * Determine if a download is a duplicate, and return metadata.
 */
exports.test = async record => {
  const [agent, threat, center] = await Promise.all([
    useragent.look(record.remoteAgent),
    domainthreat.look(record.remoteReferrer),
    datacenter.look(record.remoteIp),
  ]);
  if (record.download && record.download.isDuplicate) {
    return { isDuplicate: true, cause: record.download.cause || 'unknown', agent };
  } else if (agent.bot) {
    return { isDuplicate: true, cause: 'bot', agent };
  } else if (threat) {
    return { isDuplicate: true, cause: 'domainthreat', agent };
  } else if (center.provider) {
    // dovetail.prx.org gives datacenters a single listener-id, so don't dedup here
    return { isDuplicate: false, cause: `datacenter: ${center.provider}`, agent };
  } else {
    return { isDuplicate: false, cause: null, agent };
  }
};

/**
 * Test an impression within the download record
 */
exports.testImpression = async (record, impression) => {
  const [agent, threat, center] = await Promise.all([
    useragent.look(record.remoteAgent),
    domainthreat.look(record.remoteReferrer),
    datacenter.look(record.remoteIp),
  ]);
  if (impression.isDuplicate) {
    return { isDuplicate: true, cause: impression.cause || 'unknown', agent };
  } else if (agent.bot) {
    return { isDuplicate: true, cause: 'bot', agent };
  } else if (threat) {
    return { isDuplicate: true, cause: 'domainthreat', agent };
  } else if (center.provider) {
    // dovetail.prx.org gives datacenters a single listener-id, so don't dedup here
    return { isDuplicate: false, cause: `datacenter: ${center.provider}`, agent };
  } else {
    return { isDuplicate: false, cause: null, agent };
  }
};
