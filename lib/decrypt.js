import AWS from "aws-sdk";
import clientOptions from "./aws-options";

const kms = new AWS.KMS(clientOptions);

/**
 * Attempt to decrypt any ENV variables, and memoize the result
 */
export const decryptAws = (val) => {
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
