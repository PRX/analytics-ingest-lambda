'use strict';

const zlib = require('zlib');
const { promisify } = require('util');
const logger = require('./logger');

function shallow_flatten(arr) {
  return [].concat.apply([], arr);
}

exports.getRecordsFromEvent = async function (event) {
  // flatten in the log filter case: an event record contains a set of records.
  return shallow_flatten(
    await Promise.all(
      event.Records.map(async r => {
        try {
          return JSON.parse(Buffer.from(r.kinesis.data, 'base64').toString('utf-8'));
        } catch (decodeErr) {
          // In the case that our kinesis data is base64 + gzipped,
          // it is coming from the dovetail-router log filter subscription.
          try {
            const buffer = Buffer.from(r.kinesis.data, 'base64');
            const unzipped = await promisify(zlib.gunzip)(buffer);
            return JSON.parse(unzipped).logEvents.map(logLine => {
              // possible formats:
              //   "<json>" (ECS tasks)
              //   "<time>\t<guid>\t<json>" (old Lambdas)
              //   "<time>\t<guid>\t<level>\t<json>" (newer Lambdas)
              const parts = logLine.message.split('\t');
              if (parts.length === 3 || parts.length === 4) {
                return JSON.parse(parts[parts.length - 1]);
              } else {
                return JSON.parse(logLine.message);
              }
            });
          } catch (decodeErr) {
            logger.error(`Invalid record input: ${decodeErr} ${JSON.stringify(r)}`);
            return null;
          }
        }
      }),
    ),
  );
};
