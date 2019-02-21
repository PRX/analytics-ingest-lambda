'use strict';

// build a base64-encoded kinesis record
exports.buildRecord = (record) => {
  return {
    eventSource: 'aws:kinesis',
    eventVersion: '1.0',
    kinesis: {
      data: new Buffer(JSON.stringify(record), 'utf-8').toString('base64')
    }
  };
};

// build an event of multiple kinesis records
exports.buildEvent = (records) => {
  return {
    Records: records.map(r => exports.buildRecord(r))
  };
};
