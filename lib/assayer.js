'use strict'

const geolocate = require('./assays/geolocate')
const useragent = require('./assays/useragent')

/**
 * Determine if a download is a duplicate, and return metadata.
 */
exports.test = async (record, includeGeo = false) => {
  const [geo, agent] = await Promise.all([
    includeGeo ? geolocate.look(record.remoteIp) : null,
    useragent.look(record.remoteAgent)
  ])
  if (record.download && record.download.isDuplicate) {
    return {isDuplicate: true, cause: record.download.cause || 'unknown', geo, agent}
  } else if (agent.bot) {
    return {isDuplicate: true, cause: 'bot', geo, agent}
  } else {
    return {isDuplicate: false, cause: null, geo, agent}
  }
}

/**
 * Test an impression within the download record
 */
exports.testImpression = async (record, impression, includeGeo = false) => {
  const [geo, agent] = await Promise.all([
    includeGeo ? geolocate.look(record.remoteIp) : null,
    useragent.look(record.remoteAgent)
  ])
  if (impression.isDuplicate) {
    return {isDuplicate: true, cause: impression.cause || 'unknown', geo, agent}
  } else if (agent.bot) {
    return {isDuplicate: true, cause: 'bot', geo, agent}
  } else {
    return {isDuplicate: false, cause: null, geo, agent}
  }
}
