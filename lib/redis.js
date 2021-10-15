'use strict';

const IORedis = require('ioredis');
const logger = require('./logger');
const timestamp = require('./timestamp');

/**
 * Redis connection encapsulation (have to connect/quit on every execution)
 */
module.exports = class Redis {

  // fully assempled keys
  static podcastDownloads(dtim) { return Redis.keys(dtim).map(k => `castle:downloads.podcasts.${k}`); }
  static episodeDownloads(dtim) { return Redis.keys(dtim).map(k => `castle:downloads.episodes.${k}`); }
  static impressions(dtim) { return `dovetail:impression:${timestamp.toISODateString(dtim)}:actuals`; }

  // Get all cache key suffixes for a timestamp
  static keys(dtim) {
    let epoch;
    if (typeof(dtim) === 'number') {
      epoch = timestamp.toEpochSeconds(dtim);
      dtim = new Date(epoch * 1000);
    } else if (typeof(dtim) === 'string' && dtim.match(/^[0-9]+$/)) {
      epoch = timestamp.toEpochSeconds(parseInt(dtim));
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

  constructor(redisUrl, redisTTL = 7200, maxRetries = 5) {
    this.disconnected = true;
    this.url = redisUrl;
    this.ttl = redisTTL;
    this.conn = Redis.buildConn(redisUrl, maxRetries);
    this.conn.on('connect', () => this.disconnected = false);
    this.conn.on('error', err => {
      logger.error('Redis error:', err);
      this.disconnected = true;
    });
  }

  static buildConn(redisUrl, maxRetries) {
    const opts = {
      lazyConnect: true,
      maxRetriesPerRequest: maxRetries,
      retryStrategy: times => {
        if (times <= maxRetries) {
          return Math.min(times * 50, 2000);
        } else {
          this.disconnected = true;
          return null;
        }
      }
    };

    // optionally run in cluster mode
    if (redisUrl && redisUrl.match(/^cluster:\/\/.+:[0-9]+$/)) {
      const parts = redisUrl.replace(/^cluster:\/\//, '').split(':');
      const config = [{host: parts[0], port: parts[1]}];
      return new IORedis.Cluster(config, {lazyConnect: true, clusterRetryStrategy: opts.retryStrategy, redisOptions: opts});
    } else {
      return new IORedis(redisUrl, opts);
    }
  }

  get connected() {
    return !this.disconnected;
  }

  hostName() {
    if (this.url && !this.url.match(/^(redis)|(cluster):\/\//)) {
      return 'redis://' + this.url.replace(/^.*\/\//, '').replace(/:[0-9]+$/, '');
    } else {
      return (this.url || '').replace(/:[0-9]+$/, '') || null;
    }
  }

  async disconnect() {
    try {
      this.disconnected = true;
      await this.conn.quit();
      return true;
    } catch (err) {
      logger.error('Redis client error:', err);
      return false;
    }
  }

  increment(key, field, count) {
    if (this.disconnected) {
      this.conn.connect().catch(err => null);
    }
    return this.conn.hincrby(key, field, count);
  }

  expire(key) {
    if (this.disconnected) {
      this.conn.connect().catch(err => null);
    }
    return this.conn.expire(key, this.ttl);
  }

}
