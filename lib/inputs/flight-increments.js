'use strict';

const http = require('http');
const https = require('https');
const logger = require('../logger');
const assayer = require('../assayer');
const pingurl = require('../pingurl');
const timestamp = require('../timestamp');

/**
 * POST realtime flight increments to each region's dovetail-router
 */
module.exports = class FlightIncrements {
  constructor(records, timeout, timeoutWait) {
    this._records = (records || []).filter(r => this.check(r));
    this._timeout = timeout;
    this._timeoutWait = timeoutWait;
    this._agents = { http: new http.Agent(), https: new https.Agent() };
  }

  check(record) {
    if (record.type === 'combined' || record.type === 'postbytes') {
      return (record.impressions || []).some(i => !i.isDuplicate);
    } else {
      return false;
    }
  }

  routerConfig() {
    const hosts = (process.env.DOVETAIL_ROUTER_HOSTS || '').split(',');
    const tokens = (process.env.DOVETAIL_ROUTER_API_TOKENS || '').split(',');
    return hosts
      .map((host, index) => {
        const token = tokens[index] || null;
        if (host && host.startsWith('http')) {
          return { host, token, url: `${host}/api/v1/flight_increments` };
        } else if (host && host.match(/localhost|127\.0\.0\.1/)) {
          return { host, token, url: `http://${host}/api/v1/flight_increments` };
        } else if (host) {
          return { host, token, url: `https://${host}/api/v1/flight_increments` };
        } else {
          return null;
        }
      })
      .filter(r => r);
  }

  async insert() {
    if (this._records.length == 0) {
      return [];
    }

    // track total incremented count
    let count = 0;

    // group flight increments by date: {'2022-06-23': {'123': 1}}
    const increments = {};
    for (const rec of this._records) {
      const date = timestamp.toISODateString(rec.timestamp || 0);
      for (const imp of rec.impressions || []) {
        const { isDuplicate } = await assayer.testImpression(rec, imp);
        if (!isDuplicate) {
          increments[date] = increments[date] || {};
          increments[date][imp.flightId] = (increments[date][imp.flightId] || 0) + 1;
          count++;
        }
      }
    }

    // POST each date-increments to each dovetail-router
    const routers = this.routerConfig();
    const pings = routers
      .map(router => {
        return Object.keys(increments).map(date => {
          return this.increment(`${router.url}/${date}`, increments[date], router.token);
        });
      })
      .flat();
    await Promise.all(pings);

    // cleanup sockets (lambda executions seem to need this periodically)
    this._agents.http.destroy();
    this._agents.https.destroy();

    // return total counts incremented on each host
    return routers.map(r => ({ dest: r.host, count }));
  }

  async increment(url, authToken, data) {
    try {
      await pingurl.post(url, data, authToken, this._timeout, this._timeoutWait, this._agents);
    } catch (err) {
      logger.warn(`PINGFAIL ${err}`, { url });
    }
  }
};
