'use strict';
const fs = require('fs');
const http = require('http');
const targz = require('targz');

const URL = 'http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz';
const DB_DIR = `${__dirname}/../db`;
const GZ_FILE = `${DB_DIR}/GeoLite2-City.tar.gz`;
const DB_FILE = `${DB_DIR}/GeoLite2-City.mmdb`;

// check for existing file
try {
  fs.mkdirSync(DB_DIR);
} catch (e) {}
try {
  fs.statSync(DB_FILE);
  process.exit(0);
} catch (e) {}

// download file
let file = fs.createWriteStream(GZ_FILE);
http.get(URL, res => {
  if (res.statusCode === 200) {
    res.pipe(file);
    res.on('error', err => console.error(err) && process.exit(1));
    res.on('end', () => extract());
  } else {
    console.error(`Got ${res.statusCode} from ${URL}`);
    process.exit(1);
  }
});

// extract database file
function extract() {
  targz.decompress({src: GZ_FILE, dest: DB_DIR, tar: {
    map: h => (h.name = h.name.split('/').pop()) && h,
    ignore: name => !name.match('GeoLite2-City.mmdb')
  }}, err => {
    if (err) {
      console.error(err);
      process.exit(1);
    } else {
      fs.unlinkSync(GZ_FILE);
      fs.statSync(DB_FILE);
    }
  });
}
