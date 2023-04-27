'use strict';

const domainthreat = require('./assays/domainthreat');
const datacenter = require('./assays/datacenter');

/**
 * Determine if a download is a duplicate, and return metadata.
 */
exports.test = async record => {
  const [threat, center] = await Promise.all([
    domainthreat.look(record.remoteReferrer),
    datacenter.look(record.remoteIp),
  ]);
  if (record.download && record.download.isDuplicate) {
    return { isDuplicate: true, cause: record.download.cause || 'unknown' };
  } else if (threat) {
    return { isDuplicate: true, cause: 'domainthreat' };
  } else if (center.provider) {
    return { isDuplicate: true, cause: `datacenter: ${center.provider}` };
  } else {
    return { isDuplicate: false, cause: null };
  }
};

/**
 * Test an impression within the download record
 */
exports.testImpression = async (record, impression) => {
  const [threat, center] = await Promise.all([
    domainthreat.look(record.remoteReferrer),
    datacenter.look(record.remoteIp),
  ]);
  if (impression.isDuplicate) {
    return { isDuplicate: true, cause: impression.cause || 'unknown' };
  } else if (threat) {
    return { isDuplicate: true, cause: 'domainthreat' };
  } else if (center.provider) {
    return { isDuplicate: true, cause: `datacenter: ${center.provider}` };
  } else {
    return { isDuplicate: false, cause: null };
  }
};
