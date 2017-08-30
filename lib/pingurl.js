'use strict';

const url = require('url');
const http = require('http');
const https = require('https');

/**
 * Attempt to GET a url, with retries/error handling
 */
exports.ping = (pingUrl, timeout) => {
  if (timeout === undefined) {
    timeout = 2000;
  }
  return new Promise((resolve, reject) => {
    httpGet(pingUrl, timeout, resolve, reject, 3);
  });
}

/**
 * Retry handler
 */
function httpGet(pingUrl, timeout, resolve, reject, retries) {
  let parsed = url.parse(pingUrl);
  let getter = parsed.protocol === 'https:' ? https : http;
  if (!parsed.host) {
    return reject(new Error(`Invalid ping url: ${pingUrl}`));
  }

  // get the url, retrying timeouts
  getter.get(pingUrl, (res) => {
    if (res.statusCode === 502 && retries > 1) {
      httpGet(pingUrl, timeout, resolve, reject, retries - 1);
    } else if (res.statusCode !== 200) {
      reject(new Error(`HTTP ${res.statusCode} from ${pingUrl}`));
    } else {
      resolve(true);
    }
  }).on('error', err => {
    reject(err);
  }).setTimeout(timeout, function() {
    this.abort();
    reject(new Error(`HTTP timeout from ${pingUrl}`));
  });
}
