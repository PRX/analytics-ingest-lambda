'use strict';

const URI = require('urijs');
const URITemplate = require('urijs/src/URITemplate');
const crypto = require('crypto');
const AdPosition = require('./ad-position');
const iputil = require('./iputil');
const timestamp = require('./timestamp');
const MAXINT = 2147483647; // assume signed 32-bit int

/**
 * Count requests made to each hostname
 */
exports.count = (accumulator, pingUrl) => {
  if (typeof pingUrl === 'string') {
    let host = URI(pingUrl).hostname();
    accumulator[host] = (accumulator[host] || 0) + 1;
  }
  return accumulator;
};

/**
 * Expand url templates
 */
exports.expand = (tpl, data = {}) => {
  if (tpl.indexOf('{') === -1) {
    return tpl;
  }

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

  const adPosition = new AdPosition(data);
  Object.keys(TPL_AD_POSITION_PARAMS).forEach(key => {
    params[key] = TPL_AD_POSITION_PARAMS[key](adPosition);
  });

  return URI.expand(tpl, params).toString();
};

// param mapping for templates
const TPL_PARAMS = {
  ad: 'adId',
  agent: 'remoteAgent',
  agentmd5: 'remoteAgent',
  campaign: 'campaignId',
  creative: 'creativeId',
  episode: 'feederEpisode',
  flight: 'flightId',
  ip: 'remoteIp',
  ipmask: 'remoteIp',
  ipv4: 'remoteIp',
  listener: 'listenerId',
  listenerepisode: 'listenerEpisode',
  podcast: 'feederPodcast',
  randomstr: 'timestamp',
  randomint: 'timestamp',
  referer: 'remoteReferrer',
  timestamp: 'timestamp',
  url: 'url',
};

// clean up some of the values
const TPL_TRANSFORMS = {
  agentmd5: ua =>
    crypto
      .createHash('md5')
      .update(ua || '')
      .digest('hex'),
  ip: ip => iputil.clean(ip),
  ipv4: ip => iputil.ipV4Only(iputil.clean(ip)),
  ipmask: ip => iputil.mask(iputil.clean(ip)),
  timestamp: ts => timestamp.toEpochMilliseconds(ts),
  randomint: (timestamp, params) => Math.floor(Math.random() * MAXINT),
  randomstr: (timestamp, params) => {
    let hmac = crypto.createHmac('sha256', 'the-secret-key');
    hmac.update(`${timestamp}-${params.listenerepisode}-${params.ad}`);
    return hmac.digest('base64').replace(/\+|\/|=/g, function (match) {
      if (match == '+') {
        return '-';
      }
      if (match == '/') {
        return '_';
      }
      return '';
    });
  },
  url: (url, p) => {
    if (url && url[0] === '/') {
      return `dovetail.prxu.org${url}`;
    } else {
      return `dovetail.prxu.org/${url}`;
    }
  },
};

// NOTE: proof of concept params
const TPL_AD_POSITION_PARAMS = {
  totalduration: pos => pos.totalDuration(),
  totaladduration: pos => pos.totalAdDuration(),
  totaladpods: pos => pos.totalAdPods(),
  adpodposition: pos => pos.adPodPosition(),
  adpodoffsetstart: pos => pos.adPodOffsetStart(),
  adpodoffsetprevious: pos => pos.adPodOffsetPrevious(),
  adpodoffsetnext: pos => pos.adPodOffsetNext(),
  adpodduration: pos => pos.adPodDuration(),
  adposition: pos => pos.adPosition(),
  adpositionoffset: pos => pos.adPositionOffset(),
};
