'use strict';
const ip = require('ip');
const maxmind = require('maxmind');
const dbfile = `${__dirname}/../db/GeoLite2-City.mmdb`;

// maxmind singleton
let _maxdb;
function maxdb() {
  if (_maxdb) {
    return _maxdb;
  } else {
    return _maxdb = new Promise((resolve, reject) => {
      maxmind.open(dbfile, (err, lookup) => {
        if (err) {
          reject(err);
        } else {
          resolve(lookup);
        }
      });
    });
  }
}

/**
 * Sanity check ip addresses
 */
exports.clean = (ipString) => {
  let parts = (ipString || '').split(',').map(s => s.trim());
  return parts.filter(s => s && (ip.isV4Format(s) || ip.isV6Format(s)))[0];
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
    return null;
  }
}

/**
 * Lookup the geoid of the city geo name for an IP
 */
exports.look = (ipString) => {
  return maxdb().then(
    maxdb => {
      let cleaned = exports.clean(ipString);
      if (cleaned) {
        let loc = maxdb.get(cleaned);
        return {
          city: (loc && loc.city) ? loc.city.geoname_id : null,
          country: (loc && loc.country) ? loc.country.geoname_id : null,
          postal: (loc && loc.postal) ? loc.postal.code : null,
          latitude: (loc && loc.location) ? loc.location.latitude : null,
          longitude: (loc && loc.location) ? loc.location.longitude : null,
          masked: exports.mask(cleaned)
        };
      } else {
        return {city: null, country: null, postal: null, latitude: null, longitude: null, masked: null};
      }
    },
    err => {
      console.error('Error loading maxmind db:', err);
      return {city: null, country: null, postal: null, latitude: null, longitude: null, masked: null};
    }
  );
};
