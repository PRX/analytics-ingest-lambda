'use strict';

const PrxIpFilter = require('prx-ip-filter');

/**
 * Check if an IP is within a known datacenter
 */
let _filter;
exports.look = async (ipString) => {
  if (!_filter) {
    _filter = await PrxIpFilter.fromFile(`${__dirname}/../../db/datacenters.json`);
  }

  const match = _filter.matchRange(ipString);
  if (match) {
    return {start: match.start, end: match.end, provider: match.name};
  } else {
    return {start: null, end: null, provider: null};
  }
};
