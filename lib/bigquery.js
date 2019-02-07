'use strict';

const {BigQuery} = require('@google-cloud/bigquery');
const decrypt = require('./decrypt');
let _cachedDataset;
let _bqKey;

/**
 * Memoize the dataset we're using for this env
 */
exports.dataset = (forceRefresh) => {
  if (forceRefresh || !_cachedDataset) {
    let key = process.env.BQ_PRIVATE_KEY || '';
    let keyPromise;

    // the key may be encrypted out in aws - note that decrypted keys will come
    // out a bit funky, with their newlines encoded
    if (_bqKey && !forceRefresh) {
      keyPromise = Promise.resolve(_bqKey);
    } else if (key.match(/^\"{0,1}-----/)) {
      keyPromise = Promise.resolve(key.replace(/\\n/g, '\n').replace(/\"/, ''));
    } else {
      keyPromise = decrypt.decryptAws(key).then(decrypted => {
        return decrypted.replace(/\\n/g, '\n');
      });
    }

    return keyPromise.then(key => {
      // save the key so we don't have to decrypt every time
      _bqKey = key;

      const bigquery = new BigQuery({
        projectId: process.env.BQ_PROJECT_ID,
        credentials: {
          client_email: process.env.BQ_CLIENT_EMAIL,
          private_key: key
        }
      });
      const dataset = bigquery.dataset(process.env.BQ_DATASET);
      return _cachedDataset = dataset;
    });
  } else {
    return Promise.resolve(_cachedDataset);
  }
};

/**
 * Streaming inserts (returns promise)
 */
exports.insert = (table, rows, retries) => {
  if (retries === undefined) {
    retries = 2;
  }
  if (rows.length === 0) {
    return Promise.resolve(0);
  }
  return exports.dataset().then(dataset => {
    return dataset.table(table).insert(rows, {raw: true});
  }).then(
    res => {
      return rows.length;
    },
    err => {
      if (err.message.match(/client_email/)) {
        throw new Error('You forgot to set BQ_CLIENT_EMAIL');
      } else if (err.message.match(/private_key/)) {
        throw new Error('You forgot to set BQ_PRIVATE_KEY');
      } else if (err.message.match(/PEM_read_bio/)) {
        throw new Error('You have a poorly formatted BQ_PRIVATE_KEY');
      } else if (err.message.match(/invalid_client/)) {
        throw new Error('Invalid BQ_CLIENT_EMAIL and/or BQ_PRIVATE_KEY');
      } else if (err.message.match(/without a project ID/)) {
        throw new Error('You forgot to set BQ_PROJECT_ID');
      } else if (err.message.match(/Not Found/)) {
        throw new Error(`Could not find table: ${err.response.req.path}`)
      } else if (retries > 0) {
        return exports.insert(table, rows, retries - 1);
      } else {
        throw err;
      }
    }
  );
};
