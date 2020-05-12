'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const { BatchGet, BatchWrite } = require('@aws/dynamodb-batch-iterator');
const logger = require('./logger');
const sts = new AWS.STS();
const region = process.env.AWS_REGION || 'us-east-1';
const clientOptions = {region, maxRetries: 5, httpOptions: {timeout: 1000}}

// optionally load a client using a role
exports.client = async () => {
  if (process.env.DDB_ROLE) {
    try {
      const RoleArn = process.env.DDB_ROLE;
      const RoleSessionName = 'analytics-ingest-lambda-dynamodb';
      const data = await sts.assumeRole({RoleArn, RoleSessionName}).promise();
      const {AccessKeyId, SecretAccessKey, SessionToken} = data.Credentials;
      return new AWS.DynamoDB({...clientOptions, accessKeyId: AccessKeyId, secretAccessKey: SecretAccessKey, sessionToken: SessionToken});
    } catch (err) {
      logger.error(`STS Error [${process.env.DDB_ROLE} => ${process.env.DDB_TABLE}]: ${err}`);
      return new AWS.DynamoDB(clientOptions);
    }
  } else {
    return new AWS.DynamoDB(clientOptions);
  }
};

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

  const results = [];
  const keys = uniqueIds.map(id => [process.env.DDB_TABLE, {id: {S: id}}]);
  const client = await exports.client()
  try {
    for await (const item of new BatchGet(client, keys)) {
      results.push(item)
    }
  } catch (err) {
    err.message = `DDB ${process.env.DDB_TABLE} - ${err.message}`;
    throw err;
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
  const formatted = await Promise.all(records.map(r => exports.formatWrite(r, idField)));
  formatted.forEach(fmt => keys[fmt.PutRequest.Item.id.S] = [process.env.DDB_TABLE, fmt]);

  let numDone = 0;
  const client = await exports.client();
  try {
    for await (const item of new BatchWrite(client, Object.values(keys))) {
      numDone++;
    }
  } catch (err) {
    err.message = `DDB ${process.env.DDB_TABLE} - ${err.message}`;
    throw err;
  }
  return numDone;
};

/**
 * Format a record for write to dynamodb, with an optional expiration epoch
 */
exports.formatWrite = async (rec, idField = 'id') => {
  const id = rec[idField];
  const Item = {id: {S: id}, payload: {B: await deflate(rec, idField)}};
  const ttl = +process.env.DDB_TTL;
  if (ttl > 0) {
    Item.expiration = {N: (Math.round(new Date().getTime() / 1000) + ttl).toString()};
  }
  return {PutRequest: {Item}};
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
  const iter = new BatchWrite(await exports.client(), keys);
  while (true) {
    try {
      const result = await iter.next();
      if (result.done === false) {
        numDone++;
      } else {
        break;
      }
    } catch (err) {
      err.message = `DDB ${process.env.DDB_TABLE} - ${err.message}`;
      throw err;
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
