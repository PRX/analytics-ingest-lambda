'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
const logger = require('./logger');
const clientOptions = require('./aws-options');
const sts = new AWS.STS(clientOptions);

// optionally load a client using a role
exports.client = async () => {
  if (process.env.DDB_LOCAL) {
    return new AWS.DynamoDB({
      ...clientOptions,
      apiVersion: "2012-08-10",
      accessKeyId: "None",
      secretAccessKey: "None",
      region: "local",
      endpoint: `http://${process.env.DDB_LOCAL}:8000`
    });
  } else if (process.env.DDB_ROLE) {
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
exports.updateItemPromise = (params, client) => client.updateItem(params).promise();
exports.update = async (id, payload = null, segments = [], client = null) => {
  client = client || (await exports.client());
  const params = await exports.updateParams(id, payload, segments);
  const result = await exports.updateItemPromise(params, client);
  return exports.updateResult(id, payload, segments, result);
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
