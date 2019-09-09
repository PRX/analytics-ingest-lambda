'use strict'

const geolocate = require('./assays/geolocate')
const useragent = require('./assays/useragent')
const domainthreat = require('./assays/domainthreat')
const datacenter = require('./assays/datacenter')

/**
 * Determine if a download is a duplicate, and return metadata.
 */
exports.test = async (record, includeGeo = false) => {
  const [geo, agent, threat, center] = await Promise.all([
    includeGeo ? geolocate.look(record.remoteIp) : null,
    useragent.look(record.remoteAgent),
    domainthreat.look(record.remoteReferrer),
    datacenter.look(record.remoteIp)
  ])
  if (record.download && record.download.isDuplicate) {
    return {isDuplicate: true, cause: record.download.cause || 'unknown', geo, agent}
  } else if (agent.bot) {
    return {isDuplicate: true, cause: 'bot', geo, agent}
  } else if (threat) {
    return {isDuplicate: true, cause: 'domainthreat', geo, agent}
  } else if (center.provider) {
    // dovetail.prx.org gives datacenters a single listener-id, so don't dedup here
    return {isDuplicate: false, cause: `datacenter: ${center.provider}`, geo, agent}
  } else {
    return {isDuplicate: false, cause: null, geo, agent}
  }
}

/**
 * Test an impression within the download record
 */
exports.testImpression = async (record, impression, includeGeo = false) => {
  const [geo, agent, threat, center] = await Promise.all([
    includeGeo ? geolocate.look(record.remoteIp) : null,
    useragent.look(record.remoteAgent),
    domainthreat.look(record.remoteReferrer),
    datacenter.look(record.remoteIp)
  ])
  if (impression.isDuplicate) {
    return {isDuplicate: true, cause: impression.cause || 'unknown', geo, agent}
  } else if (agent.bot) {
    return {isDuplicate: true, cause: 'bot', geo, agent}
  } else if (threat) {
    return {isDuplicate: true, cause: 'domainthreat', geo, agent}
  } else if (center.provider) {
    // dovetail.prx.org gives datacenters a single listener-id, so don't dedup here
    return {isDuplicate: false, cause: `datacenter: ${center.provider}`, geo, agent}
  } else {
    return {isDuplicate: false, cause: null, geo, agent}
  }
}
