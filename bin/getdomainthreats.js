'use strict';

const fs = require('fs');
const util = require('util');
const https = require('https');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();

const OUTFILE = `${__dirname}/../db/domainthreats.json`;
const HTTP_LISTS = [
  'https://isc.sans.edu/feeds/suspiciousdomains_High.txt',
  'https://isc.sans.edu/feeds/suspiciousdomains_Medium.txt'
];
const BUCKET = 'prx-dovetail';
const PREFIX = 'config/domainthreats';

// load lists from the web and S3, and save to db/domainthreats.json
async function run() {
  console.log('Loading domain threats...');
  const domains = [];

  // load http lists
  for (let url of HTTP_LISTS) {
    const data = await httpGet(url);
    let count = 0;
    data.split('\n').forEach(line => {
      const parsed = parseDomain(line);
      if (parsed) {
        domains.push(parsed);
        count++;
      }
    });
    console.log(`  ${url} -> ${count}`);
  }

  // scan for S3 lists
  const resp = await s3.listObjects({Bucket: BUCKET, Prefix: PREFIX}).promise();
  const keys = resp.Contents.map(c => c.Key)
  for (let key of keys) {
    const resp = await s3.getObject({Bucket: BUCKET, Key: key}).promise();
    const data = resp.Body.toString();
    let count = 0;
    data.split('\n').forEach(line => {
      const parsed = parseDomain(line);
      if (parsed) {
        domains.push(parsed);
        count++;
      }
    });
    console.log(`  s3://${BUCKET}/${key} -> ${count}`);
  }

  console.log(`Writing ${domains.length} domains to db/domainthreats.json`);
  await util.promisify(fs.writeFile)(OUTFILE, JSON.stringify(domains));
}

// helper to get as promise
function httpGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      if (res.statusCode === 200) {
        const body = [];
        res.on('data', chunk => body.push(chunk));
        res.on('end', () => resolve(body.join('')));
      } else {
        reject(new Error(`Got ${res.statusCode} from ${URL}`));
      }
    });
  });
}

// check if a line is a domain name (vs comments/blanks/etc)
function parseDomain(str) {
  const trim = str.trim();
  if (trim && trim.match(/^[a-z0-9-.]+\.[a-z0-9-.]+$/i)) {
    return trim;
  }
}

run().then(res => console.log('Done'), err => {
  console.error('ERROR:');
  console.error(err);
  process.exit(1);
});
