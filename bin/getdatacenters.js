'use strict';

const fs = require('fs');
const util = require('util');
const neatCsv = require('neat-csv');
const ipaddr = require('ipaddr.js');
const IPCIDR = require('ip-cidr');
const netparser = require('netparser');
const s3 = new (require('aws-sdk')).S3();

const PREFIX_PARTS = 2;
const DB_DIR = `${__dirname}/../db`;
const Bucket = 'prx-dovetail';
const Prefix = 'config/datacenters';

// load datacenter csvs from S3
async function run() {
  const data = await s3.listObjects({Bucket, Prefix}).promise();
  const keys = data.Contents.map(c => c.Key).filter(k => k.endsWith('.csv'));

  // sanity check
  if (data.Contents.length < 1) {
    console.error(`Got 0 CSVs at s3://${Bucket}/${Prefix}*`);
    process.exit(1);
  }
  if (data.Contents.length > 10) {
    console.error(`Got > 10 CSVs at s3://${Bucket}/${Prefix}*`);
    process.exit(1);
  }

  // load actual csv datas
  const csvs = await Promise.all(keys.map(async (Key) => {
    const resp = await s3.getObject({Bucket, Key}).promise();
    return await neatCsv(resp.Body.toString(), {headers: ['0', '1', '2', '3']});
  }));
  const rowCount = csvs.map(c => c.length).reduce((a, b) => a + b, 0);
  console.log(`Loaded ${csvs.length} CSVs (${rowCount} rows) from S3`);

  // parse csvs, convert cidrs to ranges
  const rawRanges = [];
  csvs.forEach(csv => {
    csv.forEach(row => {
      if (ipaddr.isValid(row['0']) && ipaddr.isValid(row['1']) && row['2']) {
        const start = ipaddr.parse(row['0']);
        const end = ipaddr.parse(row['1']);
        if (start.kind() === end.kind()) {
          rawRanges.push([start, end, row['2']]);
        } else {
          console.warn(`  Mismatched range [${row['0']}, ${row['1']}, ${row['2']}]`);
        }
      } else if (row['0'] && row['1']) {
        const cidr = new IPCIDR(row['0']);
        if (cidr.isValid()) {
          const start = ipaddr.parse(cidr.start());
          const end = ipaddr.parse(cidr.end());
          rawRanges.push([start, end, row['1']]);
        } else {
          console.warn(`  Invalid cidr [${row['0']}, ${row['1']}]`);
        }
      } else {
        console.warn(`  Invalid row [${row['0']}, ${row['1']}, ${row['2']}, ${row['3']}]`);
      }
    });
  });

  // sort by start-ip
  const normalV4 = ip => ip.octets.map(n => `00${n}`.substr(-3, 3)).join('.');
  const normal = ip => ip.kind() === 'ipv6' ? ip.toFixedLengthString() : normalV4(ip);
  rawRanges.sort(([ipStartA], [ipStartB]) => normal(ipStartA) < normal(ipStartB) ? -1 : 1);
  console.log(`Parsed ${rawRanges.length} IP ranges`);

  // combine ranges, warning when different providers overlap
  const condensed = [];
  rawRanges.forEach(([startIp, endIp, provider]) => {
    const prev = condensed.length - 1;
    if (prev < 0) {
      condensed.push([startIp, endIp, provider]);
    } else {
      const [prevStartIp, prevEndIp, prevProvider] = condensed[prev];
      if (normal(startIp) <= normal(prevEndIp)) {
        if (provider === prevProvider) {
          condensed[prev][1] = normal(endIp) > normal(prevEndIp) ? endIp : prevEndIp;
        } else {
          console.warn(`  Skipping row: [${startIp}, ${endIp}, ${provider}]`);
          console.warn(`       Keeping: [${prevStartIp}, ${prevEndIp}, ${prevProvider}]`);
        }
      } else {
        condensed.push([startIp, endIp, provider]);
      }
    }
  });
  console.log(`Condensed to ${condensed.length} IP ranges`);

  // normalize providers/ranges
  const providers = [...new Set(condensed.map(range => range[2]))];
  const ranges = condensed.map(([startIp, endIp, provider]) => {
    return [normal(startIp), normal(endIp), providers.indexOf(provider)];
  });
  console.log(`Normalized ${providers.length} providers`);

  // key by prefix (first 2 blocks) for fast lookup
  const lookup = {};
  ranges.forEach(([start, end, provider], idx) => {
    const sep = start.indexOf('.') > -1 ? '.' : ':';

    // get networks in range
    const netBits = (sep === '.' ? 8 : 16) * PREFIX_PARTS;
    const allNets = [start];
    while (allNets[allNets.length - 1] < end) {
      const prev = allNets[allNets.length - 1];
      const next = netparser.nextNetwork(`${prev}/${netBits}`).replace(`/${netBits}`, '');
      allNets.push(normal(ipaddr.parse(next)));
    }

    // look for unique prefixes
    const prefixes = allNets.map(ip => ip.split(sep).slice(0, PREFIX_PARTS).join(sep));
    const uniques = [...new Set(prefixes)];

    // add to lookup
    uniques.forEach(prefix => {
      lookup[prefix] = lookup[prefix] || [];
      lookup[prefix].push(idx);
    });
  });
  console.log(`Built lookup of ${Object.keys(lookup).length} prefixes`);

  // write to json lookup file
  const json = JSON.stringify({providers, ranges, lookup});
  await util.promisify(fs.writeFile)(`${DB_DIR}/datacenters.json`, json);
  console.log(`Wrote ${json.length} bytes to db/datacenters.json`);
}

run().then(res => console.log('Done'), err => {
  console.error('ERROR:');
  console.error(err);
  process.exit(1);
});
