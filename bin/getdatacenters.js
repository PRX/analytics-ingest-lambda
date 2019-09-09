'use strict';

const PrxIpFilter = require('prx-ip-filter');

const OUTFILE = `${__dirname}/../db/datacenters.json`;
const BUCKET = 'prx-dovetail';
const PREFIX = 'config/datacenters';

// load datacenter csvs from S3, and save to db/datacenters.json
async function run() {
  const filter = await PrxIpFilter.fromS3CSV(BUCKET, PREFIX);
  if (filter.names.length < 1) {
    console.error(`Got 0 datacenters from s3://${BUCKET}/${PREFIX}*`);
    process.exit(1);
  }
  await filter.toFile(OUTFILE);
  console.log(`Wrote ${filter.names.length} datacenters to db/datacenters.json`);
}

run().then(res => console.log('Done'), err => {
  console.error('ERROR:');
  console.error(err);
  process.exit(1);
});
