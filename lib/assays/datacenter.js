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
    const [fixed, kind] = iputil.fixedKind(cleaned);
    const rangesToSearch = centers[kind] || [];

    // binary search for datacenters
    let low = 0;
    let high = rangesToSearch.length - 1;
    while (high >= low) {
      const probe = Math.floor((high + low) / 2);
      const [startIp, endIp, idx] = rangesToSearch[probe];
      if (startIp > fixed) {
        high = probe - 1;
      } else if (endIp < fixed) {
        low = probe + 1;
      } else {
        return {start: startIp, end: endIp, provider: centers.providers[idx]};
      }
    }
  }

  // not found in datacenters
  return {start: null, end: null, provider: null}
};
