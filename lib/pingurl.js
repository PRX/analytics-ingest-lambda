'use strict';

const url = require('url');
const followRedirects = require('follow-redirects');
followRedirects.maxRedirects = 10;
const http = followRedirects.http;
const https = followRedirects.https;
const logger = require('./logger');
const iputil = require('./iputil');

const TIMEOUT_MS = 5000;
const TIMEOUT_WAIT_MS = 1000;
const MAX_ATTEMPTS = 3;

/**
 * Attempt to GET a url, with retries/error handling
 */
exports.ping = async (pingUrl, inputData, timeout, timeoutWait) => {
  let parsed = url.parse(pingUrl);
  if (!parsed.host) {
    throw new Error(`Invalid ping url: ${pingUrl}`);
  }
  if (parsed.hostname === 'tps.doubleverify.com') {
    throw new Error(`tps.doubleverify.com is not our friend`);
  }

  // options
  const opts = {
    url: pingUrl,
    host: parsed.hostname,
    path: parsed.path,
    port: parsed.port,
    headers: exports.parseHeaders(inputData)
  };
  timeout = (timeout === undefined) ? TIMEOUT_MS : timeout;
  timeoutWait = (timeoutWait === undefined) ? TIMEOUT_WAIT_MS : timeoutWait;

  // stop being sloooooooow
  if (host === 'sink.pdst.fm') {
    timeout = 1000;
  }

  // get with retries
  for (let i = 1; i <= MAX_ATTEMPTS; i++) {
    try {
      await getAsPromise(opts, timeout);
      return true;
    } catch (err) {
      if (i < MAX_ATTEMPTS && err.statusCode >= 500) {
        logger.warn(`PINGRETRY ${err.statusCode} ${opts.url}`);
        await new Promise(resolve => setTimeout(resolve, timeoutWait));
      } else {
        throw err;
      }
    }
  }
};

// set headers to match original requester
exports.parseHeaders = (data) => {
  let headers = {};
  if (data && data.remoteAgent) {
    headers['User-Agent'] = data.remoteAgent;
  }
  if (data && data.remoteIp) {
    const clean = iputil.cleanAll(data.remoteIp);
    if (clean) {
      headers['X-Forwarded-For'] = iputil.maskLeft(clean);
    }
  }
  if (data && data.remoteReferrer) {
    headers['Referer'] = data.remoteReferrer;
  }
  return headers;
};

// http.get as a promise
function getAsPromise(opts, timeoutMs) {
  return new Promise((resolve, reject) => {
    const getter = opts.url.match(/^https/) ? https : http;
    const req = getter.get(opts, (res) => {
      if (res.statusCode >= 300) {
        const err = new Error(`HTTP ${res.statusCode} from ${opts.url}`);
        err.statusCode = res.statusCode;
        reject(err);
      } else {
        resolve(true);
      }
    }).on('error', reject).setTimeout(timeoutMs, () => {
      req.abort();
      reject(new Error(`HTTP timeout from ${opts.url}`));
    });
  });
}
