'use strict';

const { parse } = require('url');
const followRedirects = require('follow-redirects');
followRedirects.maxRedirects = 10;
const http = followRedirects.http;
const https = followRedirects.https;
const logger = require('./logger');
const iputil = require('./iputil');

const TIMEOUT_MS = 5000;
const TIMEOUT_WAIT_MS = 1000;
const MAX_ATTEMPTS = 3;

const sleep = ms => new Promise(r => setTimeout(r, ms));
const getAgent = (url, a) => (a ? (url.match(/^https/) ? a.https : a.http) : undefined);

/**
 * Attempt to GET a url, with retries/error handling
 */
exports.ping = async (url, inputData, timeout, timeoutWait, agents) => {
  const opts = {
    method: 'GET',
    headers: exports.parseHeaders(inputData),
    agent: getAgent(url, agents),
  };
  return exports.request(url, opts, null, timeout, timeoutWait);
};

/**
 * Make a request with timeouts/retries
 */
exports.request = async (url, opts, data, timeout, timeoutWait) => {
  timeout = timeout === undefined ? TIMEOUT_MS : timeout;
  timeoutWait = timeoutWait === undefined ? TIMEOUT_WAIT_MS : timeoutWait;

  // special cases
  const parsed = parse(url);
  if (!parsed.host) {
    throw new Error(`Invalid ping url: ${url}`);
  }
  if (parsed.host === 'tps.doubleverify.com') {
    throw new Error(`tps.doubleverify.com is not our friend`);
  }
  if (parsed.host === 'sink.pdst.fm') {
    timeout = 1000;
  }

  // retry 5XXs
  for (let i = 1; i <= MAX_ATTEMPTS; i++) {
    try {
      await requestAsPromise(url, opts, data, timeout);
      return true;
    } catch (err) {
      if (i < MAX_ATTEMPTS && err.statusCode >= 500) {
        logger.warn(`PINGRETRY ${err.statusCode} ${url}`);
        await sleep(timeoutWait);
      } else if (i < MAX_ATTEMPTS && err.code === 'EMFILE') {
        // TODO: does waiting/retrying these even help them succeed?
        logger.warn(`EMFILE retry ${opts.url}`);
        await sleep(100);
        await sleep(timeoutWait);
      } else {
        throw err;
      }
    }
  }
};

// set headers to match original requester
exports.parseHeaders = data => {
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
function requestAsPromise(url, opts, data, timeoutMs) {
  return new Promise((resolve, reject) => {
    const proto = url.match(/^https/) ? https : http;
    const req = proto
      .request(url, opts, res => {
        if (res.statusCode >= 300) {
          const err = new Error(`HTTP ${res.statusCode} from ${opts.url}`);
          err.statusCode = res.statusCode;
          reject(err);
        } else {
          resolve(true);
        }
      })
      .on('error', err => {
        // https://github.com/node-modules/agentkeepalive#support-reqreusedsocket
        // TODO: is this even helpful?
        if (req.reusedSocket && err.code === 'ECONNRESET') {
          const err = new Error(`Reused socket for ${opts.url}`);
          err.reusedSocket = true;
          reject(err);
        } else {
          reject(err);
        }
      })
      .setTimeout(timeoutMs, () => {
        req.abort();
        reject(new Error(`HTTP timeout from ${opts.url}`));
      });

    // optional request body
    if (data) {
      req.write(data);
    }
    req.end();
  });
}
