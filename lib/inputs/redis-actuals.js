'use strict'

const logger = require('../logger')
const assayer = require('../assayer')
const Redis = require('../redis')

/**
 * Increment redis actual impressions for Dovetail Router
 */
module.exports = class RedisActuals {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r))
    this._redis = new Redis(process.env.REDIS_IMPRESSIONS_HOST, process.env.REDIS_IMPRESSIONS_TTL)
  }

  check(record) {
    if (record.type === 'combined' || record.type === 'postbytes') {
      return (record.impressions || []).some(i => i.targetPath && !i.isDuplicate)
    } else {
      return false
    }
  }

  async insert() {
    if (this._records.length == 0) {
      return []
    }

    // get flight ids to increment
    const flightIncrs = {}
    let totalIncrements = 0
    await this.eachImpression(async (r, i) => {
      const {isDuplicate} = await assayer.testImpression(r, i)
      if (i.targetPath && i.flightId && !isDuplicate) {
        const key = Redis.impressions(r.timestamp)
        flightIncrs[key] = flightIncrs[key] || {}
        flightIncrs[key][i.flightId] = (flightIncrs[key][i.flightId] || 0) + 1
        totalIncrements++
      }
    })

    // short circuit if this won't work
    if (totalIncrements === 0 || !this._redis.hostName()) {
      return Promise.resolve([])
    }

    // increment and expire redis hashes in parallel
    const increments = Object.keys(flightIncrs).map(k => this.doIncrements(k, flightIncrs[k]))
    const expires = Object.keys(flightIncrs).map(k => this.doExpire(k))
    try {
      await Promise.all(increments.concat(expires))
    } catch (err) {
      logger.error(`Redis error: ${err}`)
      totalIncrements = 0
    }

    // disconnect redis and return counts
    await this._redis.disconnect()
    return [{count: totalIncrements, dest: this._redis.hostName()}]
  }

  doIncrements(key, fieldsToCounts) {
    return Promise.all(Object.keys(fieldsToCounts).map(field => {
      return this._redis.increment(key, field, fieldsToCounts[field])
    }))
  }

  doExpire(key) {
    return this._redis.expire(key)
  }

  async eachImpression(handler) {
    await Promise.all(this._records.map(async (rec) => {
      await Promise.all(rec.impressions.map(async (imp) => {
        await handler(rec, imp)
      }))
    }))
  }

}
