'use strict';

const AWS = require('aws-sdk');
const ssm = new AWS.SSM({ region: process.env.AWS_REGION || 'us-east-1' });
const logger = require('./logger');

// only load params once
let PARAMS_LOADED = false;

/**
 * Bootstrap parameter-store into the process.env
 */
exports.load = (callback, nextToken) => {
  if (process.env.PARAMSTORE_PREFIX) {
    if (PARAMS_LOADED) {
      return callback();
    } else {
      getParameters(params => {
        params.forEach(param => {
          let key = param.Name.split('/').pop();
          process.env[key] = param.Value;
        });
        PARAMS_LOADED = true;
        callback();
      });
    }
  } else {
    callback();
  }
};

/**
 * SSM calls, logging/ignoring errors
 */
function getParameters(callback, prevResults, nextToken) {
  prevResults = prevResults || [];
  let params = { Path: process.env.PARAMSTORE_PREFIX, WithDecryption: true };
  if (nextToken) {
    params.NextToken = nextToken;
  }
  ssm.getParametersByPath(params, (err, data) => {
    if (err) {
      logger.error(`SSM Error: ${err}`);
      callback(prevResults);
    } else if (data.NextToken) {
      getParameters(callback, data.Parameters.concat(prevResults), data.NextToken);
    } else {
      callback(data.Parameters.concat(prevResults));
    }
  });
}
