const AWS = require("aws-sdk");
const clientOptions = require("./aws-options");
const kms = new AWS.KMS(clientOptions);

/**
 * Attempt to decrypt any ENV variables, and memoize the result
 */
exports.decryptAws = (val) => {
  return new Promise((resolve, reject) => {
    const opts = { CiphertextBlob: Buffer.from(val, "base64") };
    kms.decrypt(opts, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data.Plaintext.toString("ascii"));
      }
    });
  });
};
