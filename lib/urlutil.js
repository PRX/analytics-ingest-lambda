'use strict';

const URI = require('urijs');
const URITemplate = require('urijs/src/URITemplate');
const crypto = require('crypto');
const lookip = require('./lookip');
const timestamp = require('./timestamp');
const MAXINT = 2147483647; // assume signed 32-bit int

/**
 * Count requests made to each hostname
 */
exports.count = (accumulator, pingUrl) => {
  if (typeof(pingUrl) === 'string') {
    let host = URI(pingUrl).hostname();
    accumulator[host] = (accumulator[host] || 0) + 1;
  }
  return accumulator;
}

/**
 * Expand url templates
 */
exports.expand = (tpl, data) => {
  let params = {};
  Object.keys(TPL_PARAMS).forEach(key => {
    let val = data[TPL_PARAMS[key]];
    if (TPL_TRANSFORMS[key]) {
      val = TPL_TRANSFORMS[key](val, params);
    }
    if (val !== null && val !== undefined) {
      params[key] = val;
    }
  });
  return URI.expand(tpl, params).toString();
}

// param mapping for templates
const TPL_PARAMS = {
  ip:        'remoteIp',
  agent:     'remoteAgent',
  agentmd5:  'remoteAgent',
  referer:   'remoteReferrer',
  ad:        'adId',
  campaign:  'campaignId',
  creative:  'creativeId',
  flight:    'flightId',
  timestamp: 'timestamp',
  episode:   'feederEpisode',
  podcast:   'feederPodcast',
  uuid:      'requestUuid',
  randomstr: 'requestUuid',
  randomint: 'requestUuid'
};

//

// clean up some of the values
const TPL_TRANSFORMS = {
  agentmd5:  ua => crypto.createHash('md5').update(ua || '').digest('hex'),
  ip:        ip => lookip.clean(ip),
  timestamp: ts => timestamp.toEpochMilliseconds(ts),
  randomint: (uuid, params) => Math.floor(Math.random() * MAXINT),
  randomstr: (uuid, params) => {
    let hmac = crypto.createHmac('sha256', 'the-secret-key');
    hmac.update(`${uuid}-${params.ad}`);
    return hmac.digest('base64').replace(/\+|\/|=/g, function (match) {
      if (match == '+') { return '-'; }
      if (match == '/') { return '_'; }
      return '';
    });
  }
}
