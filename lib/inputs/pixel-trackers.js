const crypto = require('crypto')
const logger = require('../logger')
const timestamp = require('../timestamp')
const lookip = require('../lookip')
const bigquery = require('../bigquery')

/**
 * Pixel.prx.org trackers
 */
module.exports = class PixelTrackers {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r))
  }

  check(record) {
    return record.type === 'pixel'
  }

  async insert() {
    if (this._records.length === 0) {
      return []
    }

    // format records and organize by dataset + table
    const formatted = await Promise.all(this._records.map(r => this.format(r)))
    const grouped = this._records.reduce((acc, rec, idx) => {
      const dest = this.destination(rec)
      if (dest) {
        (acc[dest] = acc[dest] || []).push(formatted[idx])
      }
      return acc
    }, {})

    // insert in parallel
    return await Promise.all(Object.keys(grouped).map(async (datasetTable) => {
      try {
        const [ds, tbl] = datasetTable.split('.')
        const num = await bigquery.insert(ds, tbl, grouped[datasetTable])
        return {count: num, dest: datasetTable}
      } catch (err) {
        if (err.code === 404) {
          logger.error(`Table not found: ${datasetTable}`)
          return {count: 0, dest: datasetTable}
        } else {
          const insertErrors = logger.combineErrors(err)
          if (insertErrors) {
            logger.error(`Insert errors on ${datasetTable}: ${insertErrors}`)
            return {count: 0, dest: datasetTable}
          } else {
            throw err
          }
        }
      }
    }))
  }

  async format(record) {
    const epoch = timestamp.toEpochSeconds(record.timestamp || 0)
    const userId = this.userId(record)
    const geo = await lookip.look(record.remoteIp)
    return {
      insertId: this.insertId(epoch, userId, record.key, record.canonical),
      json: {
        timestamp:          epoch,
        user_id:            userId,
        key:                record.key,
        canonical:          record.canonical,
        remote_agent:       record.remoteAgent,
        remote_referrer:    record.remoteReferrer,
        remote_ip:          geo.masked,
        city_geoname_id:    geo.city,
        country_geoname_id: geo.country,
        postal_code:        geo.postal,
        latitude:           geo.latitude,
        longitude:          geo.longitude
      }
    }
  }

  userId(rec) {
    const parts = [rec.remoteAgent, rec.remoteIp, rec.remoteReferrer].join('-')
    return crypto.createHash('md5').update(parts).digest('hex')
  }

  insertId(epoch, userId, key, canonical) {
    const parts = [epoch, userId, key, canonical].join('-')
    return crypto.createHash('md5').update(parts).digest('hex')
  }

  destination(rec) {
    const parts = (rec.destination || '').split('.')
    if (parts.length === 2 && parts[0] && parts[1]) {
      return `${parts[0]}.${parts[1]}`
    } else if (parts.length === 1 && parts[0] && process.env.BQ_DATASET) {
      return `${process.env.BQ_DATASET}.${parts[0]}`
    } else {
      logger.error(`No destination in record: ${JSON.stringify(rec)}`)
      return null
    }
  }

}
