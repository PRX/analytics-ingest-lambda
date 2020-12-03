'use strict';

const maxmind = require('maxmind');
const iputil = require('../iputil');
const dbfile = `${__dirname}/../../db/GeoLite2-City.mmdb`;

// maxmind singleton
let _maxdb;
async function maxdb() {
  if (_maxdb) {
    return _maxdb;
  } else {
    return _maxdb = await maxmind.open(dbfile);
  }
}

/**
 * Lookup the geoid of the city geo name for an IP
 */
exports.look = (ipString) => {
  return maxdb().then(
    maxdb => {
      let cleaned = iputil.clean(ipString);
      if (cleaned) {
        let loc = maxdb.get(cleaned);
        return {
          city: (loc && loc.city) ? loc.city.geoname_id : null,
          country: (loc && loc.country) ? loc.country.geoname_id : null,
          postal: (loc && loc.postal) ? loc.postal.code : null,
          latitude: (loc && loc.location) ? loc.location.latitude : null,
          longitude: (loc && loc.location) ? loc.location.longitude : null,
          masked: iputil.mask(cleaned)
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
