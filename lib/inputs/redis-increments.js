'use strict';

const logger = require('../logger');
const assayer = require('../assayer');
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
    const isType = record.type === 'combined' || record.type === 'postbytes';
    const isFeeder = record.feederPodcast || record.feederEpisode;
    const isDownload = !!record.download && !record.download.isDuplicate;
    return isType && isFeeder && isDownload;
  }

  async insert() {
    let keysToFields = {}, totalIncrements = 0;
    await Promise.all(this._records.map(async (r) => {
      const {isDuplicate} = await assayer.test(r);
      if (!isDuplicate && r.feederPodcast) {
        totalIncrements += this.addKeys(keysToFields, this.getPodcastKeys(r), r.feederPodcast);
      }
      if (!isDuplicate && r.feederEpisode) {
        totalIncrements += this.addKeys(keysToFields, this.getEpisodeKeys(r), r.feederEpisode);
      }
    }));

    if (totalIncrements === 0 || !this._redis.hostName()) {
      return Promise.resolve([]);
    } else {
      let increments = Object.keys(keysToFields).map(k => this.doIncrements(k, keysToFields[k]));
      let expires = Object.keys(keysToFields).map(k => this.doExpire(k));
      return Promise.all(increments.concat(expires)).then(() => {
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
    return Redis.podcastDownloads(record.timestamp);
  }

  getEpisodeKeys(record) {
    return Redis.episodeDownloads(record.timestamp);
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

};
