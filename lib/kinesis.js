'use strict';

const crypto = require('crypto');
const AWS = require('aws-sdk');

// kinesis client (exported for testing/mocking)
exports.client = new AWS.Kinesis({region: process.env.AWS_REGION || 'us-east-1'});
exports.stream = () => {
  if (process.env.KINESIS_STREAM) {
    return process.env.KINESIS_STREAM.replace(/^.+stream\//, '');
  } else {
    return null;
  }
};
exports.retryStream = () => {
  if (process.env.KINESIS_RETRY_STREAM) {
    return process.env.KINESIS_RETRY_STREAM.replace(/^.+stream\//, '');
  } else {
    return null;
  }
};

/**
 * Batch put records to kinesis
 */
exports.put = (datas, maxChunk = 200) => {
  if (!exports.stream()) {
    throw new Error('You must set a KINESIS_STREAM');
  }
  return putInChunks(exports.stream(), datas, maxChunk);
};
exports.putRetry = (datas, maxChunk = 200) => {
  if (!exports.retryStream()) {
    throw new Error('You must set a KINESIS_RETRY_STREAM');
  }
  return putInChunks(exports.retryStream(), datas, maxChunk);
};

/**
 * Chunk input and call kinesis.putRecords
 */
async function putInChunks(streamName, datas, maxChunk) {
  if (!datas || datas.length === 0) {
    return 0;
  }
  if (!Array.isArray(datas)) {
    return await putInChunks(streamName, [datas], maxChunk);
  }
  if (datas.length > maxChunk) {
    const chunks = chunkArray(datas, maxChunk);
    const nums = await Promise.all(chunks.map(c => putInChunks(streamName, c, maxChunk)));
    return nums.reduce((a, b) => a + b, 0);
  }

  // format records and insert
  const Records = datas.map(r => dataToRecord(r));
  await exports.client.putRecords({StreamName: streamName, Records}).promise();
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
  } else if (data.listenerEpisode) {
    return {Data: json, PartitionKey: data.listenerEpisode};
  } else {
    let hmac = crypto.createHmac('sha256', 'the-secret-key');
    hmac.update(JSON.stringify(data));
    return {Data: json, PartitionKey: hmac.digest('base64')};
  }
}
