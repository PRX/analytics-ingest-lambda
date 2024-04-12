'use strict';

const http = require('http');
const https = require('https');
const logger = require('../logger');
const assayer = require('../assayer');
const pingurl = require('../pingurl');
const urlutil = require('../urlutil');

/**
 * Ping 3rd party pingbacks
 */
module.exports = class Pingbacks {
  constructor(records, timeout, timeoutWait) {
    this._records = (records || []).filter(r => this.check(r));
    this._timeout = timeout;
    this._timeoutWait = timeoutWait;
    this._agents = { http: new http.Agent(), https: new https.Agent() };
  }

  check(record) {
    return (
      record.type === 'postbytes' &&
      (record.impressions || []).some(i => i.pings && i.pings.length && !i.isDuplicate)
    );
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // filter duplicates
    let hostCounts = {};
    let pings = [];
    await this.eachImpression(async (r, i) => {
      const { isDuplicate } = await assayer.testImpression(r, i);
      if (!isDuplicate) {
        (i.pings || []).forEach(pb => {
          try {
            const url = urlutil.expand(pb, { ...r, ...i });
            pings.push(this.ping(url, r, hostCounts));
          } catch (err) {
            logger.warn(`PINGFAIL ${err}`, { url: pb });
          }
        });
      }
    });

    // ping urls
    await Promise.all(pings);

    // cleanup sockets (lambda executions seem to need this periodically)
    this._agents.http.destroy();
    this._agents.https.destroy();

    // incrementing the counts per-host
    return Object.keys(hostCounts).map(host => {
      return { dest: host, count: hostCounts[host] };
    });
  }

  async ping(url, record, hostCounts) {
    try {
      await pingurl.ping(url, record, this._timeout, this._timeoutWait, this._agents);
      logger.info('PINGED', { url });
      urlutil.count(hostCounts, url);
    } catch (err) {
      logger.warn(`PINGFAIL ${err}`, { url });
    }
  }

  async eachImpression(handler) {
    await Promise.all(
      this._records.map(async rec => {
        await Promise.all(
          rec.impressions.map(async imp => {
            await handler(rec, imp);
          }),
        );
      }),
    );
  }
};
