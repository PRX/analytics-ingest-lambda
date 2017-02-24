/**
 * Does this look like a dovetail impression record?
 */
exports.check = (record) => {
  return record.adId !== undefined && record.impressionSent !== undefined;
};

/**
 * Convert to bigquery table format
 */
exports.toOutput = (record) => {
  return {
    _uuid:           record.requestUuid,
    ad_id:           record.adId,
    campaign_id:     record.campaignId,
    creative_id:     record.creativeId,
    flight_id:       record.flightId,
    path:            record.path,
    program:         record.program,
    remote_agent:    record.remoteAgent,
    remote_ip:       record.remoteIp,
    timestamp:       record.timestamp,
    impression_sent: record.impressionSent,
    request_uuid:    record.requestUuid,
    is_duplicate:    record.isDuplicate
  };
};
