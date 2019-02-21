'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const { BatchGet, BatchWrite } = require('@aws/dynamodb-batch-iterator');

// ddb client (exported for testing/mocking)
exports.client = new AWS.DynamoDB({region: process.env.AWS_REGION || 'us-east-1'});

/**
 * Batch read items from dynamodb
 */
exports.get = async (ids, idField = 'id') => {
  if (!idField) {
    throw new Error('You must specify an id field');
  }
  if (!process.env.DDB_TABLE) {
    throw new Error('You must set a DDB_TABLE');
  }
  if (!Array.isArray(ids)) {
    const recs = await exports.get([ids], idField);
    return recs[0];
  }

  // duplicate ids cause errors
  const uniqueIds = ids.filter((id, idx) => ids.indexOf(id) === idx);

  // TODO: async iterators not supported in node 8.10
  const results = [];
  const keys = uniqueIds.map(id => [process.env.DDB_TABLE, {id: {S: id}}]);
  const iter = new BatchGet(exports.client, keys);
  while (true) {
    try {
      const result = await iter.next();
      if (result.done === false) {
        results.push(result.value);
      } else {
        break;
      }
    } catch (err) {
      throw new Error(`DDB ${process.env.DDB_TABLE} - ${err.message}`);
    }
  }

  // inflate/reassemble payload, and sort by input order
  const keyedResults = {};
  await Promise.all(results.map(async ([, data]) => {
    const id = data.id.S;
    const buffer = data.payload.B;
    keyedResults[id] = await inflate(buffer, id, idField);
  }));
  return ids.map(id => keyedResults[id] || null);
};

/**
 * Batch write items to dynamodb
 */
exports.write = async (records, idField = 'id') => {
  if (!idField) {
    throw new Error('You must specify an id field');
  }
  if (!process.env.DDB_TABLE) {
    throw new Error('You must set a DDB_TABLE');
  }
  if (!Array.isArray(records)) {
    return await exports.write([records], idField);
  }
  if (!records.every(r => r[idField])) {
    throw new Error(`All records must include the field '${idField}'`);
  }

  // compress payload and assemble DDB put requests
  const keys = {};
  await Promise.all(records.map(async (rec) => {
    const id = rec[idField];
    keys[id] = [
      process.env.DDB_TABLE,
      {
        PutRequest: {Item: {id: {S: id}, payload: {B: await deflate(rec, idField)}}}
      }
    ];
  }));

  // TODO: async iterators not supported in node 8.10
  let numDone = 0;
  const iter = new BatchWrite(exports.client, Object.values(keys));
  while (true) {
    try {
      const result = await iter.next();
      if (result.done === false) {
        numDone++;
      } else {
        break;
      }
    } catch (err) {
      throw new Error(`DDB ${process.env.DDB_TABLE} - ${err.message}`);
    }
  }
  return numDone;
};

/**
 * Batch delete items (for tests)
 */
exports.delete = async (ids) => {
  if (!process.env.DDB_TABLE) {
    throw new Error('You must set a DDB_TABLE');
  }
  if (!Array.isArray(ids)) {
    return await exports.delete([ids]);
  }

  // duplicate ids cause errors
  const uniqueIds = ids.filter((id, idx) => ids.indexOf(id) === idx);
  const keys = uniqueIds.map(id => {
    return [
      process.env.DDB_TABLE,
      {DeleteRequest: {Key: {id: {S: id}}}}
    ];
  });

  // TODO: async iterators not supported in node 8.10
  let numDone = 0;
  const iter = new BatchWrite(exports.client, keys);
  while (true) {
    try {
      const result = await iter.next();
      if (result.done === false) {
        numDone++;
      } else {
        break;
      }
    } catch (err) {
      throw new Error(`DDB ${process.env.DDB_TABLE} - ${err.message}`);
    }
  }
  return numDone;
};

/**
 * Promisify deflate/inflate calls
 */
function inflate(buffer, idValue, idField) {
  return new Promise((resolve, reject) => {
    zlib.inflate(buffer, (err, result) => {
      if (err) {
        reject(err);
      } else {
        try {
          const payload = JSON.parse(result);
          payload[idField] = idValue;
          resolve(payload);
        } catch (probablyParseErr) {
          reject(probablyParseErr);
        }
      }
    });
  });
}
function deflate(payload, idField) {
  const tmpId = payload[idField];

  delete payload[idField];
  const json = JSON.stringify(payload);
  payload[idField] = tmpId;

  return new Promise((resolve, reject) => {
    zlib.deflate(json, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}
