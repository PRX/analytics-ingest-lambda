'use strict';

const AWS = require('aws-sdk');
const kms = new AWS.KMS();

/**
 * Attempt to decrypt any ENV variables, and memoize the result
 */
exports.decryptAws = (val) => {
  return new Promise((resolve, reject) => {
    let opts = {CiphertextBlob: new Buffer(val, 'base64')};
    kms.decrypt(opts, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data.Plaintext.toString('ascii'));
      }
    });
  });
};
