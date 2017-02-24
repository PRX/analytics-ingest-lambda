'use strict';

const BQ = require('@google-cloud/bigquery');
const uuid = require('uuid');
const decrypt = require('./decrypt');
let _cachedDataset;

/**
 * Memoize the dataset we're using for this env
 */
exports.dataset = () => {
  if (!_cachedDataset) {
    let keyPromise = Promise.resolve(process.env.BQ_PRIVATE_KEY);

    // the key may be encrypted out in aws - note that decrypted keys will come
    // out a bit funky, with their newlines encoded
    if (process.env.BQ_PRIVATE_KEY && !process.env.BQ_PRIVATE_KEY.match(/^-----/)) {
      keyPromise = decrypt.decrypt(process.env.BQ_PRIVATE_KEY).then(decrypted => {
        return process.env.BQ_PRIVATE_KEY = decrypted.replace(/\\n/g, '\n');
      });
    }

    return keyPromise.then(key => {
      return _cachedDataset = BQ({
        projectId: process.env.BQ_PROJECT_ID,
        credentials: {client_email: process.env.BQ_CLIENT_EMAIL, private_key: key}
      }).dataset(process.env.BQ_DATASET);
    });
  } else {
    return Promise.resolve(_cachedDataset);
  }
};

/**
 * Streaming inserts (returns promise)
 */
exports.insert = (table, rows) => {
  if (rows.length === 0) {
    return Promise.resolve(0);
  }
  let insertOpts = {raw: true};

  // use _uuid where available, or generate
  rows = rows.map(r => {
    let data = {insertId: r._uuid || uuid.v4()};
    delete r._uuid;
    data.json = r;
    return data;
  });

  return exports.dataset().then(dataset => {
    return dataset.table(table).insert(rows, insertOpts);
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
      }
      throw err;
    }
  );
};