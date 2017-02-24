'use strict';

const bigquery = require('./lib/bigquery');
const Inputs = require('./lib/inputs');

exports.handler = (event, context, callback) => {
  let errs = [];
  function concatErrors() {
    if (errs.length === 1) {
      return errs[0];
    } else if (errs.length > 1) {
      let msgs = errs.map(e => `${e}`).join(', ');
      return new Error(`Multiple errors: ${msgs}`);
    }
  }

  // decode the base64 kinesis records
  let records = event.Records.map(r => {
    return JSON.parse(new Buffer(r.kinesis.data, 'base64').toString('utf-8'));
  });

  // input records and check for unrecognized
  let inputs = new Inputs(records);
  inputs.unrecognized().forEach(r => {
    errs.push(new Error(`Unrecognized input record: ${JSON.stringify(r)}`));
  });

  // process known inputs
  let doInserts = inputs.outputsByType().map(([tableName, rows]) => {
    return bigquery.insert(tableName, rows);
  });

  // run in parallel
  Promise.all(doInserts).then(
    counts => {
      inputs.types.forEach((table, idx) => {
        if (counts[idx] > 0) {
          console.log(`Inserted ${counts[idx]} rows into ${table}`);
        }
      });
      let total = counts.reduce((a, b) => a + b, 0);
      callback(concatErrors(), `Inserted ${total} rows`);
    },
    err => {
      if (err.name === 'PartialFailureError') {
        errs.concat(err.errors);
      } else {
        errs.push(err);
      }
      callback(concatErrors());
    }
  );
};
