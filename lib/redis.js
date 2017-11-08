'use strict';

const redis = require('redis');
const logger = require('./logger');
const timestamp = require('./timestamp');

/**
 * Increment a redis hash
 */
exports.increment = (key, field, count) => {
  if (client()) {
    return new Promise((resolve, reject) => {
      client().hincrby(key, field, count, (err, reply) => {
        if (err) {
          logger.warn(`Redis HINCRBY error: ${err}`);
        }
        resolve(reply);
      });
    });
  } else {
    return Promise.resolve(null);
  }
}

/**
 * Set expiration on a redis key
 */
exports.expire = (key) => {
  let ttl = process.env.REDIS_TTL || 900;
  if (client()) {
    return new Promise((resolve, reject) => {
      client().expire(key, ttl, (err, reply) => {
        if (err) {
          logger.warn(`Redis EXPIRE error: ${err}`);
        }
        resolve(reply);
      });
    });
  } else {
    return Promise.resolve(null);
  }
}

/**
 * Fully assembled keys
 */
exports.podcastDownloads = (dtim) => exports.keys(dtim).map(k => `downloads.podcasts.${k}`);
exports.episodeDownloads = (dtim) => exports.keys(dtim).map(k => `downloads.episodes.${k}`);
exports.podcastImpressions = (dtim) => exports.keys(dtim).map(k => `impressions.podcasts.${k}`);
exports.episodeImpressions = (dtim) => exports.keys(dtim).map(k => `impressions.episodes.${k}`);

/**
 * Get all cache key suffixes for a
 */
exports.keys = (dtim) => {
  let epoch;
  if (typeof(dtim) === 'number') {
    epoch = timestamp.toEpochSeconds(dtim);
    dtim = new Date(epoch * 1000);
  } else {
    epoch = timestamp.toEpochSeconds(dtim.getTime());
  }
  let fifteen = epoch - (epoch % 900);
  let hour = epoch - (epoch % 3600);
  let day = epoch - (epoch % 86400);
  let week = beginningOfWeek(dtim, day);
  let month = beginningOfMonth(dtim, day);
  return [
    '15MIN.' + timestamp.toISOExtendedZ(fifteen),
    'HOUR.' + timestamp.toISOExtendedZ(hour),
    'DAY.' + timestamp.toISOExtendedZ(day),
    'WEEK.' + timestamp.toISOExtendedZ(week),
    'MONTH.' + timestamp.toISOExtendedZ(month)
  ];
}

// helpers
function beginningOfWeek(dtim, beginningOfDayEpoch) {
  return beginningOfDayEpoch - dtim.getUTCDay() * 86400;
}
function beginningOfMonth(dtim, beginningOfDayEpoch) {
  return beginningOfDayEpoch - (dtim.getUTCDate() - 1) * 86400;
}

// just-in-time redis connection
// only need a client if ENVs are configured
let _client;
function client() {
  if (process.env.REDIS_HOST) {
    if (!_client) {
      let hostKey = process.env.REDIS_HOST.match(/^redis:/) ? 'url' : 'host';
      _client = redis.createClient({
        fast: true,
        connect_timeout: 5000, // don't wait forever
        [hostKey]: process.env.REDIS_HOST,
        retry_strategy: (opts) => {
          if (opts.error && opts.error.code === 'ECONNREFUSED') {
            logger.error('Redis client error: ECONNREFUSED');
            return undefined;
          }
          return (opts.attempt > 10) ? undefined : 100;
        }
      });
      _client.on('error', (err) => logger.error(`Redis client error: ${err}`));
    }
    return _client;
  } else {
    _client = null;
    return null;
  }
}
exports.client = () => client();
