'use strict';

const domainthreat = require('./assays/domainthreat');
const datacenter = require('./assays/datacenter');
const logger = require('./logger');

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
    if (record.download?.cause !== 'domainthreat') {
      const analytics = 'domainthreat';
      const router = record.download?.cause || '';
      logger.warn('MISMATCHED domainthreat', { analytics, router });
    }
    return { isDuplicate: true, cause: 'domainthreat' };
  } else if (center.provider) {
    if (record.download?.cause !== `datacenter: ${center.provider}`) {
      const analytics = `datacenter: ${center.provider}`;
      const router = record.download?.cause || '';
      logger.warn(`MISMATCHED datacenter`, { analytics, router });
    }
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
