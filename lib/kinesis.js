'use strict';

const crypto = require('crypto');
const AWS = require('aws-sdk');

// kinesis client (exported for testing/mocking)
exports.client = new AWS.Kinesis({region: process.env.AWS_REGION || 'us-east-1'});
exports.stream = () => process.env.KINESIS_STREAM;

/**
 * Batch put records to kinesis
 */
exports.put = async (datas, maxChunk = 200) => {
  if (!datas || datas.length === 0) {
    return 0;
  }
  if (!Array.isArray(datas)) {
    return await exports.put([datas]);
  }
  if (datas.length > maxChunk) {
    const chunks = chunkArray(datas, maxChunk);
    const nums = await Promise.all(chunks.map(c => exports.put(c, maxChunk)));
    return nums.reduce((a, b) => a + b, 0);
  }
  if (!process.env.KINESIS_STREAM) {
    throw new Error('You must set a KINESIS_STREAM');
  }

  // format records and insert
  const StreamName = process.env.KINESIS_STREAM;
  const Records = datas.map(r => dataToRecord(r));
  await exports.client.putRecords({StreamName, Records}).promise();
  return datas.length;
};

// split array into chunks of max-size
function chunkArray(array, maxSize) {
  let i = 0;
  const chunks = [];
  const n = array.length;
  while (i < n) {
    chunks.push(array.slice(i, i += maxSize));
  }
  return chunks;
}

// format records
function dataToRecord(data) {
  const json = JSON.stringify(data);
  if (data.requestUuid) {
    return {Data: json, PartitionKey: data.requestUuid};
  } else if (data.listenerSession) {
    return {Data: json, PartitionKey: data.listenerSession};
  } else {
    let hmac = crypto.createHmac('sha256', 'the-secret-key');
    hmac.update(JSON.stringify(data));
    return {Data: json, PartitionKey: hmac.digest('base64')};
  }
}
