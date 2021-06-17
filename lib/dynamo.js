'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const logger = require('./logger');
const sts = new AWS.STS();
const region = process.env.AWS_REGION || 'us-east-1';
const clientOptions = { region, maxRetries: 5, httpOptions: { timeout: 1000 } };

// optionally load a client using a role
exports.client = async () => {
  if (process.env.DDB_ROLE) {
    try {
      const RoleArn = process.env.DDB_ROLE;
      const RoleSessionName = 'analytics-ingest-lambda-dynamodb';
      const data = await sts.assumeRole({ RoleArn, RoleSessionName }).promise();
      const { AccessKeyId, SecretAccessKey, SessionToken } = data.Credentials;
      return new AWS.DynamoDB({
        ...clientOptions,
        accessKeyId: AccessKeyId,
        secretAccessKey: SecretAccessKey,
        sessionToken: SessionToken,
      });
    } catch (err) {
      logger.error(`STS Error [${process.env.DDB_ROLE} => ${process.env.DDB_TABLE}]: ${err}`);
      return new AWS.DynamoDB(clientOptions);
    }
  } else {
    return new AWS.DynamoDB(clientOptions);
  }
};

/**
 * Update an item, returning payload and new segments
 */
exports.update = async (id, payload = null, segments = [], client = null) => {
  client = client || (await exports.client());
  const params = await exports.updateParams(id, payload, segments);
  const result = await client.updateItem(params).promise();
  return exports.updateResult(id, payload, segments, result);
};

/**
 * Update multiple items, limiting concurrency
 */
exports.updateAll = async (updates, concurrency = 25) => {
  const result = { success: [], failures: [] };
  const client = await exports.client();

  // spin up N workers, and process the queue of updates
  async function worker() {
    let args;
    while ((args = updates.shift())) {
      try {
        result.success.push(await exports.update.apply(this, args.concat([client])));
      } catch (err) {
        logger.error(`DDB Error [${process.env.DDB_TABLE}]: ${err}`, { args });
        result.failures.push(args);
      }
    }
  }

  const threads = Array(concurrency).fill(true);
  await Promise.all(threads.map(() => worker()));

  return result;
};

/**
 * Get item (for tests)
 */
exports.get = async id => {
  const client = await exports.client();
  const params = { Key: { id: { S: id } }, TableName: process.env.DDB_TABLE };
  return client.getItem(params).promise();
};

/**
 * Delete item (for tests)
 */
exports.delete = async id => {
  const client = await exports.client();
  const params = { Key: { id: { S: id } }, TableName: process.env.DDB_TABLE };
  return client.deleteItem(params).promise();
};

/**
 * Params for an updateItem request
 */
exports.updateParams = async (id, payload, segments) => {
  if (!process.env.DDB_TABLE) {
    throw new Error('You must set a DDB_TABLE');
  }

  const params = {
    AttributeUpdates: {},
    Key: { id: { S: id } },
    ReturnValues: 'ALL_OLD',
    TableName: process.env.DDB_TABLE,
  };

  // compress payload data
  if (payload) {
    const compressed = await exports.deflate(payload);
    params.AttributeUpdates.payload = { Action: 'PUT', Value: { B: compressed } };
  }

  // stringify json data
  if (segments && segments.length) {
    const strings = (segments || []).map(s => s.toString());
    params.AttributeUpdates.segments = { Action: 'ADD', Value: { SS: strings } };
  }

  // optional expiration
  const ttl = +process.env.DDB_TTL;
  if (ttl > 0) {
    const expires = Math.round(new Date().getTime() / 1000) + ttl;
    params.AttributeUpdates.expiration = { Action: 'PUT', Value: { N: expires.toString() } };
  }

  return params;
};

/**
 * Return the id, payload, and any "new" segments
 */
exports.updateResult = async (id, setPayload, setSegments, result) => {
  const oldPayload =
    result.Attributes && result.Attributes.payload ? result.Attributes.payload.B : null;
  const oldSegments =
    result.Attributes && result.Attributes.segments ? result.Attributes.segments.SS : [];

  // only inflate the payload if necessary
  const newPayload = setPayload || (oldPayload && (await exports.inflate(oldPayload))) || null;

  // sanitize the segments we just set
  setSegments = (setSegments || []).map(s => s.toString());

  // segments are "new" the first time they're set AND we have a non-null payload
  const newSegments = {};
  if (setPayload && !oldPayload) {
    setSegments.forEach(s => (newSegments[s] = true));
    oldSegments.forEach(s => (newSegments[s] = true));
  } else if (oldPayload) {
    setSegments.forEach(s => (newSegments[s] = true));
    oldSegments.forEach(s => (newSegments[s] = false));
  } else {
    setSegments.forEach(s => (newSegments[s] = false));
    oldSegments.forEach(s => (newSegments[s] = false));
  }

  // return null for empty segments
  if (Object.keys(newSegments).length) {
    return [id, newPayload, newSegments];
  } else {
    return [id, newPayload, null];
  }
};

/**
 * Promisify deflate/inflate calls
 */
exports.inflate = buffer => {
  return new Promise((resolve, reject) => {
    zlib.inflate(buffer, (err, result) => {
      if (err) {
        reject(err);
      } else {
        try {
          resolve(JSON.parse(result));
        } catch (probablyParseErr) {
          reject(probablyParseErr);
        }
      }
    });
  });
};
exports.deflate = payload => {
  const json = JSON.stringify(payload);
  return new Promise((resolve, reject) => {
    zlib.deflate(json, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
};
