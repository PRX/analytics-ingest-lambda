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
    if (val !== null && val !== undefined) {
      params[key] = val;
    }
  });

  Object.keys(TPL_TRANSFORMS).forEach(key => {
    params[key] = TPL_TRANSFORMS[key](params[key], params);
  });

  return URI.expand(tpl, params).toString();
}

// param mapping for templates
const TPL_PARAMS = {
  ad:        'adId',
  agent:     'remoteAgent',
  agentmd5:  'remoteAgent',
  episode:   'feederEpisode',
  campaign:  'campaignId',
  creative:  'creativeId',
  flight:    'flightId',
  host:      'host',
  ip:        'remoteIp',
  path:      'path',
  podcast:   'feederPodcast',
  program:   'program',
  protocol:  'protocol',
  query:     'query',
  randomstr: 'requestUuid',
  randomint: 'requestUuid',
  referer:   'remoteReferrer',
  timestamp: 'timestamp',
  uuid:      'requestUuid',
  url:       'path'
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
  },
  url: (path, p) => {
    let u = `${p.host}/${p.program}/${p.path}`;
    if (p.query && p.query.length > 0) {
      u = `${u}?${p.query}`
    }
    return u;
  }
}
