'use strict';

exports.handler = (event, context, callback) => {

  // decode the base64 kinesis records
  let records = event.Records.map(r => {
    return JSON.parse(new Buffer(r.kinesis.data, 'base64').toString('utf-8'));
  });

  console.log('records:', records);

  callback(null, `Processed ${records.length} records`);

};
