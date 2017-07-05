'use strict';
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
          console.log('--loaded');
          resolve(lookup);
        }
      });
    });
  }
}

// sanity check ip addresses
function cleanIp(ip) {
  return (ip || '').split(',').map(s => s.trim())
    .filter(s => s && s !== 'unknown')[0]
}

/**
 * Lookup the geoid of the city geo name for an IP
 */
exports.look = (ipString) => {
  return maxdb().then(
    maxdb => {
      let cleaned = cleanIp(ipString);
      if (cleaned) {
        let loc = maxdb.get(cleaned);
        return {
          city: (loc && loc.city) ? loc.city.geoname_id : null,
          country: (loc && loc.country) ? loc.country.geoname_id : null
        };
      } else {
        return {city: null, country: null};
      }
    },
    err => {
      console.error('Error loading maxmind db:', err);
      return {city: null, country: null};
    }
  );
};

/**
 * Get the YYYYMMDD date string for a timestamp
 */
exports.toDateString = (timestamp) => {
  let iso = new Date(exports.toEpochSeconds(timestamp) * 1000).toISOString();
  return iso.replace(/(-)|(T.+)/g, '');
};
