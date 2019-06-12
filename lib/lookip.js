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
