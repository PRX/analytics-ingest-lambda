'use strict';

const logger = require('../logger');
const Redis = require('../redis');

/**
 * Increment redis counts
 */
module.exports = class RedisIncrements {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
    this._redis = new Redis(process.env.REDIS_HOST, process.env.REDIS_TTL);
  }

  check(record) {
    return (record.type === 'download')
        && (record.feederPodcast || record.feederEpisode)
        && !record.isDuplicate;
  }

  insert() {
    let keysToFields = {}, totalIncrements = 0;
    this._records.forEach(r => {
      if (r.feederPodcast) {
        totalIncrements += this.addKeys(keysToFields, this.getPodcastKeys(r), r.feederPodcast);
      }
      if (r.feederEpisode) {
        totalIncrements += this.addKeys(keysToFields, this.getEpisodeKeys(r), r.feederEpisode);
      }
    });

    if (totalIncrements === 0 || !this._redis.hostName()) {
      return Promise.resolve([]);
    } else {
      return this._redis.connect().then(() => {
        let increments = Object.keys(keysToFields).map(k => this.doIncrements(k, keysToFields[k]));
        let expires = Object.keys(keysToFields).map(k => this.doExpire(k));
        return Promise.all(increments.concat(expires));
      }).then(replies => {
        return this._redis.disconnect();
      }).then(() => {
        return [{count: totalIncrements, dest: this._redis.hostName()}];
      }).catch(err => {
        logger.error(`Redis error: ${err}`);
        return [{count: 0, dest: this._redis.hostName()}];
      });
    }
  }

  doIncrements(key, fieldsToCounts) {
    return Promise.all(Object.keys(fieldsToCounts).map(field => {
      return this._redis.increment(key, field, fieldsToCounts[field]);
    }));
  }

  doExpire(key) {
    return this._redis.expire(key);
  }

  getPodcastKeys(record) {
    if (record.type === 'download') {
      return Redis.podcastDownloads(record.timestamp);
    } else {
      return Redis.podcastImpressions(record.timestamp);
    }
  }

  getEpisodeKeys(record) {
    if (record.type === 'download') {
      return Redis.episodeDownloads(record.timestamp);
    } else {
      return Redis.episodeImpressions(record.timestamp);
    }
  }

  addKeys(map, keys, id) {
    keys.forEach(key => {
      if (map[key]) {
        if (map[key][id]) {
          map[key][id]++;
        } else {
          map[key][id] = 1;
        }
      } else {
        map[key] = {[id]: 1};
      }
    });
    return keys.length;
  }

}
