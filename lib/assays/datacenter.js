'use strict';

const PrxIpFilter = require('prx-ip-filter');

/**
 * Check if an IP is within a known datacenter
 */
let _filter;
function loadFilter() {
  if (!_filter) {
    _filter = PrxIpFilter.fromFile(`${__dirname}/../../db/datacenters.json`);
  }
  return _filter
}

exports.look = async (ipString) => {
  const filter = await loadFilter();
  const match = filter.matchRange(ipString);
  if (match) {
    return {start: match.start, end: match.end, provider: match.name};
  } else {
    return {start: null, end: null, provider: null};
  }
};
