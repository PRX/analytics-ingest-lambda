'use strict';

const url = require('url');
const http = require('http');
const https = require('https');

const TIMEOUT_MS = 5000;
const TIMEOUT_WAIT_MS = 1000;
const MAX_ATTEMPTS = 3;

/**
 * Attempt to GET a url, with retries/error handling
 */
exports.ping = (pingUrl, inputData, timeout, timeoutWait) => {
  return new Promise((resolve, reject) => {
    let parsed = url.parse(pingUrl);
    if (!parsed.host) {
      return reject(new Error(`Invalid ping url: ${pingUrl}`));
    }

    let pinger = new HttpPing(timeout, timeoutWait);
    let opts = {
      url: pingUrl,
      host: parsed.host,
      path: parsed.path,
      port: parsed.port,
      headers: exports.parseHeaders(inputData)
    };
    pinger.get(opts, resolve, reject);
  });
}

exports.parseHeaders = (data) => {
  let headers = {};
  if (data && data.remoteAgent) {
    headers['User-Agent'] = data.remoteAgent;
  }
  if (data && data.remoteIp) {
    headers['X-Forwarded-For'] = data.remoteIp;
  }
  if (data && data.remoteReferrer) {
    headers['Referer'] = data.remoteReferrer;
  }
  return headers;
}

/**
 * Retry handler
 */
 class HttpPing {

   constructor(timeout, timeoutWait, maxAttempts) {
     this.attempt = 0;
     this.timeout = (timeout === undefined) ? TIMEOUT_MS : timeout;
     this.timeoutWait = (timeoutWait === undefined) ? TIMEOUT_WAIT_MS : timeoutWait;
     this.maxAttempts = (maxAttempts === undefined) ? MAX_ATTEMPTS : maxAttempts;
   }

   // get the url, delaying slightly between attempts
   get(opts, resolve, reject) {
     let getter = opts.url.match(/^https/) ? https : http;
     if (this.attempt === 0) {
       this.getNow(getter, opts, resolve, reject);
     } else {
       setTimeout(() => this.getNow(getter, opts, resolve, reject), this.timeoutWait);
     }
   }

   getNow(getter, opts, resolve, reject) {
     this.attempt++;
     getter.get(opts, (res) => {
       if (res.statusCode >= 500 && this.attempt < this.maxAttempts) {
         this.get(opts, resolve, reject);
       } else if (res.statusCode !== 200) {
         reject(new Error(`HTTP ${res.statusCode} from ${opts.url}`));
       } else {
         resolve(true);
       }
     }).on('error', err => {
       reject(err);
     }).setTimeout(this.timeout, function() {
       this.abort();
       reject(new Error(`HTTP timeout from ${opts.url}`));
     });
   }

}
