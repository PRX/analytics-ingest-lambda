'use strict';

const redis = require('redis');
const logger = require('./logger');
const timestamp = require('./timestamp');

/**
 * Redis connection encapsulation (have to connect/quit on every execution)
 */
module.exports = class Redis {

  // fully assempled keys
  static podcastDownloads(dtim) { return Redis.keys(dtim).map(k => `downloads.podcasts.${k}`); }
  static episodeDownloads(dtim) { return Redis.keys(dtim).map(k => `downloads.episodes.${k}`); }
  static podcastImpressions(dtim) { return Redis.keys(dtim).map(k => `impressions.podcasts.${k}`); }
  static episodeImpressions(dtim) { return Redis.keys(dtim).map(k => `impressions.episodes.${k}`); }

  // Get all cache key suffixes for a timestamp
  static keys(dtim) {
    let epoch;
    if (typeof(dtim) === 'number') {
      epoch = timestamp.toEpochSeconds(dtim);
      dtim = new Date(epoch * 1000);
    } else {
      epoch = timestamp.toEpochSeconds(dtim.getTime());
    }
    let hour = epoch - (epoch % 3600);
    let day = epoch - (epoch % 86400);
    return [
      'HOUR.' + timestamp.toISOExtendedZ(hour),
      'DAY.' + timestamp.toISOExtendedZ(day),
    ];
  }

  constructor(host, ttl) {
    this._host = host;
    this._ttl = ttl || 7200;
  }

  hostName() {
    if (this._host) {
      return 'redis://' + this._host.replace(/^.*\/\//, '').replace(/:.*$/, '');
    } else {
      return null;
    }
  }

  connect() {
    return new Promise((resolve, reject) => {
      let hostKey = (this._host || '').match(/^redis:/) ? 'url' : 'host';
      this._client = redis.createClient({
        connect_timeout: 5000, // don't wait forever
        [hostKey]: process.env.REDIS_HOST,
        retry_strategy: (opts) => {
          if (opts.error && opts.error.code === 'ECONNREFUSED') {
            logger.error('Redis client error: ECONNREFUSED');
            reject(opts.error);
          }
          if (opts.attempt < 10) {
            return 100;
          } else if (opts.error) {
            reject(opts.error);
          } else {
            reject(new Error(`Failed to connect to redis after ${opts.attempt} attempts`));
          }
          return undefined;
        }
      });
      this._client.on('error', err => reject(err));
      this._client.on('connect', () => resolve(this._client));
    });
  }

  disconnect() {
    return new Promise((resolve, reject) => {
      this._client.on('error', err => reject(err));
      this._client.on('end', () => resolve());
      this._client.quit();
    });
  }

  increment(key, field, count) {
    return new Promise((resolve, reject) => {
      this._client.hincrby(key, field, count, (err, reply) => resolve(reply));
    });
  }

  expire(key) {
    return new Promise((resolve, reject) => {
      this._client.expire(key, this._ttl, (err, reply) => resolve(reply));
    });
  }

}
