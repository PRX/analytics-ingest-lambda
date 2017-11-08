'use strict';

const logger = require('../logger');
const redis = require('../redis');

/**
 * Increment redis counts
 */
module.exports = class RedisIncrements {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return (record.type === 'download' || record.type === 'impression')
        && (record.feederPodcast || record.feederEpisode)
        && !record.isDuplicate;
  }

  insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    } else {
      let keysToFields = {}, totalIncrements = 0;

      this._records.forEach(r => {
        if (r.feederPodcast) {
          totalIncrements += this.addKeys(keysToFields, this.getPodcastKeys(r), r.feederPodcast);
        }
        if (r.feederEpisode) {
          totalIncrements += this.addKeys(keysToFields, this.getEpisodeKeys(r), r.feederEpisode);
        }
      });

      let increments = Object.keys(keysToFields).map(k => this.doIncrements(k, keysToFields[k]));
      let expires = Object.keys(keysToFields).map(k => this.doExpire(k));
      return Promise.all(increments.concat(expires)).then(replies => {
        return [{count: totalIncrements, dest: this.redisHostName()}];
      });
    }
  }

  doIncrements(key, fieldsToCounts) {
    return Promise.all(Object.keys(fieldsToCounts).map(field => {
      return redis.increment(key, field, fieldsToCounts[field]);
    }));
  }

  doExpire(key) {
    return redis.expire(key);
  }

  getPodcastKeys(record) {
    if (record.type === 'download') {
      return redis.podcastDownloads(record.timestamp);
    } else {
      return redis.podcastImpressions(record.timestamp);
    }
  }

  getEpisodeKeys(record) {
    if (record.type === 'download') {
      return redis.episodeDownloads(record.timestamp);
    } else {
      return redis.episodeImpressions(record.timestamp);
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

  redisHostName() {
    if (process.env.REDIS_HOST) {
      return 'redis://' + process.env.REDIS_HOST.replace(/^.*\/\//, '').replace(/:.*$/, '');
    } else {
      return 'redis://null';
    }
  }

}
