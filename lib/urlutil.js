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
  } else {
    return accumulator;
  }
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

// clean up some of the values
const TPL_TRANSFORMS = {
  ip:        ip => lookip.clean(ip),
  timestamp: ts => timestamp.toEpochMilliseconds(ts),
  randomstr: (uuid, params) => {
    let hmac = crypto.createHmac('sha256', 'the-secret-key');
    hmac.update(`${uuid}-${params.ad}`);
    return hmac.digest('base64').replace(/\+|\/|=/g, function (match) {
      if (match == '+') { return '-'; }
      if (match == '/') { return '_'; }
      return '';
    });
  },
  randomint: (uuid, params) => {
    let num = 0;
    for (let i = 0; i < params.randomstr.length; i++) {
      let next = parseInt(`${num}${params.randomstr.charCodeAt(i)}`);
      if (next < MAXINT) {
        num = next;
      } else {
        break;
      }
    }
    return num;
  }
}
