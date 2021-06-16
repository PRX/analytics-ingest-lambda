'use strict';

const AWS = require('aws-sdk');
const zlib = require('zlib');
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
 * Update an item, returning payload and new segments
 */
exports.update = async (id, payload = null, segments = []) => {
  const params = await exports.updateParams(id, payload, segments);
  if (Object.keys(params.AttributeUpdates).length === 0) {
    return null;
  }

  const client = await exports.client();
  const result = await client.updateItem(params).promise();
  return exports.updateResult(id, payload, segments, result);
}

/**
 * Get item (for tests)
 */
exports.get = async id => {
  const client = await exports.client()
  return client.getItem({Key: {id: {S: id}}, TableName: process.env.DDB_TABLE}).promise()
}

/**
 * Delete item (for tests)
 */
exports.delete = async id => {
  const client = await exports.client()
  return client.deleteItem({Key: {id: {S: id}}, TableName: process.env.DDB_TABLE}).promise()
}

/**
 * Params for an updateItem request
 */
exports.updateParams = async (id, payload, segments) => {
  if (!process.env.DDB_TABLE) {
    throw new Error('You must set a DDB_TABLE');
  }

  const params = {
    AttributeUpdates: {},
    Key: {id: {S: id}},
    ReturnValues: 'ALL_OLD',
    TableName: process.env.DDB_TABLE
  };

  // compress payload data
  if (payload) {
    const compressed = await exports.deflate(payload);
    params.AttributeUpdates.payload = {Action: 'PUT', Value: {B: compressed}};
  }

  // stringify json data
  if (segments && segments.length) {
    const strings = (segments || []).map(s => s.toString());
    params.AttributeUpdates.segments = {Action: 'ADD', Value: {SS: strings}};
  }

  // optional expiration
  const ttl = +process.env.DDB_TTL;
  if (ttl > 0) {
    params.AttributeUpdates.expiration = {N: (Math.round(new Date().getTime() / 1000) + ttl).toString()};
  }

  return params;
}

/**
 * Return the id, payload, and any "new" segments
 */
exports.updateResult = async (id, setPayload, setSegments, result) => {
  const oldPayload = (result.Attributes && result.Attributes.payload) ? result.Attributes.payload.B : null;
  const oldSegments = (result.Attributes && result.Attributes.segments) ? result.Attributes.segments.SS : [];

  // sanitize the segments we just set
  setSegments = (setSegments || []).map(s => s.toString());

  // segments are "new" the first time they're set AND we have a non-null payload
  const newSegments = new Set();
  if (setPayload && !oldPayload) {
    setSegments.concat(oldSegments).forEach(s => newSegments.add(s));
  } else {
    setSegments.filter(s => !oldSegments.includes(s)).forEach(s => newSegments.add(s));
  }

  // return null unless there are new segments
  const newPayload = setPayload || (oldPayload && await exports.inflate(oldPayload));
  if (newPayload && newSegments.size) {
    return [id, newPayload, Array.from(newSegments).sort()];
  } else {
    return null;
  }
}

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
}
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
}
