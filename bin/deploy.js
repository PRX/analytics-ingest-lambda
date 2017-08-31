'use strict';
const dotenv = require('dotenv');
const lambda = require('node-lambda');
const fs     = require('fs');

// environment name
let envName = process.argv[2];
if (['development', 'staging', 'production'].indexOf(envName) < 0) {
  console.error('Usage: deploy.js [development|staging|production]');
  process.exit(1);
}

// make sure maxmind is downloaded
try {
  fs.statSync(`${__dirname}/../db/GeoLite2-City.mmdb`);
} catch (e) {
  console.error('Geolite database not downloaded - run "npm run geolite".');
  process.exit(1);
}

// HACKY: don't try to reconfigure the function - just upload the new code
let oldParams = lambda._params;
lambda._params = function(program, buffer) {
  let params = oldParams.call(this, program, buffer);
  return {FunctionName: params.FunctionName, Code: params.Code};
};

// deploy 2 functions, for bigquery-ingest vs pingbacks:
const EXCLUDE = 'event.json .env env-example *.log .git .gitignore test';
['analytics-ingest', 'analytics-pingback'].forEach(name => {
  let globs = EXCLUDE;
  if (name === 'analytics-pingback') {
    globs = `${globs} db`; // don't need maxmind db for pingbacks
  }
  lambda.deploy({
    environment: envName,
    region: process.env.AWS_REGION || 'us-east-1',
    functionName: name,
    excludeGlobs: globs
  });
});
