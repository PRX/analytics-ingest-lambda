'use strict';

const ipaddr = require('ipaddr.js');
const iputil = require('../iputil');
const centers = require('../../db/datacenters.json');
const numParts = centers.lookupParts || 2;

/**
 * Check if an IP is within a known datacenter
 */
exports.look = async (ipString) => {
  const cleaned = iputil.clean(ipString);
  if (cleaned) {
    const fixed = iputil.fixed(cleaned);
    const sep = fixed.indexOf('.') > -1 ? '.' : ':';
    const prefix = fixed.split(sep).slice(0, numParts).join(sep);

    // only continue if prefix is mapped
    if (centers.lookup[prefix]) {
      const ranges = centers.lookup[prefix].map(i => centers.ranges[i]);
      for (let i = 0; i < ranges.length; i++) {
        const [start, end, providerIdx] = ranges[i];
        if (start <= fixed && fixed <= end) {
          return {
            start: start,
            end: end,
            provider: centers.providers[providerIdx]
          }
        }
      }
    }
  }

  // not found in datacenters
  return {start: null, end: null, provider: null}
};
