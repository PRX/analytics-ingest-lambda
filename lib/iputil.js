'use strict';

const ip = require('ip');

/**
 * Sanity check ip addresses
 */
exports.clean = (ipString) => {
  return exports.cleanAll(ipString, false)[0];
}
exports.cleanAll = (xffString, join = true) => {
  let parts = (xffString || '').split(',').map(s => s.trim());
  let cleaned = parts.filter(s => s && (ip.isV4Format(s) || ip.isV6Format(s)));
  if (join) {
    return cleaned.join(', ') || undefined;
  } else {
    return cleaned;
  }
}

/**
 * Bitmask ip to remove last octet(s)
 */
exports.mask = (cleanIpString) => {
  if (ip.isV4Format(cleanIpString)) {
    return ip.mask(cleanIpString, '255.255.255.0');
  } else if (ip.isV6Format(cleanIpString)) {
    return ip.mask(cleanIpString, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:0');
  } else {
    return cleanIpString;
  }
}
exports.maskLeft = (cleanXffString) => {
  const parts = (cleanXffString || '').split(', ');
  parts[0] = exports.mask(parts[0]);
  return parts.join(', ');
}
