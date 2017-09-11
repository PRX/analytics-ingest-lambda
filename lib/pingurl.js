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
exports.ping = (pingUrl, timeout, timeoutWait) => {
  return new Promise((resolve, reject) => {
    let pinger = new HttpPing(timeout, timeoutWait);
    pinger.get(pingUrl, resolve, reject);
  });
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

   get(pingUrl, resolve, reject) {
     let parsed = url.parse(pingUrl);
     let getter = parsed.protocol === 'https:' ? https : http;
     if (!parsed.host) {
       return reject(new Error(`Invalid ping url: ${pingUrl}`));
     }

     // get the url, delaying slightly between attempts
     if (this.attempt === 0) {
       this.getNow(getter, pingUrl, resolve, reject);
     } else {
       setTimeout(() => this.getNow(getter, pingUrl, resolve, reject), this.timeoutWait);
     }
   }

   getNow(getter, pingUrl, resolve, reject) {
     this.attempt++;
     getter.get(pingUrl, (res) => {
       if (res.statusCode === 502 && this.attempt < this.maxAttempts) {
         this.get(pingUrl, resolve, reject);
       } else if (res.statusCode !== 200) {
         reject(new Error(`HTTP ${res.statusCode} from ${pingUrl}`));
       } else {
         resolve(true);
       }
     }).on('error', err => {
       reject(err);
     }).setTimeout(this.timeout, function() {
       this.abort();
       reject(new Error(`HTTP timeout from ${pingUrl}`));
     });
   }

}
