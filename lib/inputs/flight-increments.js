import http from "node:http";
import https from "node:https";
import logger from "../logger";
import pingurl from "../pingurl";
import timestamp from "../timestamp";

/**
 * POST realtime flight increments to each region's dovetail-router
 */
export default class FlightIncrements {
  constructor(records, timeout, timeoutWait) {
    this._records = (records || []).filter((r) => this.check(r));
    this._timeout = timeout;
    this._timeoutWait = timeoutWait;
    this._agents = { http: new http.Agent(), https: new https.Agent() };
  }

  check(record) {
    return record.type === "postbytes" && (record.impressions || []).some((i) => !i.isDuplicate);
  }

  routerConfig() {
    const hosts = (process.env.DOVETAIL_ROUTER_HOSTS || "").split(",");
    const tokens = (process.env.DOVETAIL_ROUTER_API_TOKENS || "").split(",");
    return hosts
      .map((host, index) => {
        const token = tokens[index] || null;
        if (host?.startsWith("http")) {
          return { host, token, url: `${host}/api/v1/flight_increments` };
        } else if (host?.match(/localhost|127\.0\.0\.1/)) {
          return { host, token, url: `http://${host}/api/v1/flight_increments` };
        } else if (host) {
          return { host, token, url: `https://${host}/api/v1/flight_increments` };
        } else {
          return null;
        }
      })
      .filter((r) => r);
  }

  async insert() {
    if (this._records.length === 0) {
      return [];
    }

    // track total incremented count
    let count = 0;

    // group flight increments by date: {'2022-06-23': {'123': 1}}
    const increments = {};
    for (const rec of this._records) {
      const date = timestamp.toISODateString(rec.timestamp || 0);
      for (const imp of rec.impressions || []) {
        if (!imp.isDuplicate) {
          increments[date] = increments[date] || {};
          increments[date][imp.flightId] = (increments[date][imp.flightId] || 0) + 1;
          count++;
        }
      }
    }

    // POST each date-increments to each dovetail-router
    const routers = this.routerConfig();
    const pings = routers.flatMap((router) => {
      return Object.keys(increments).map((date) => {
        return this.increment(`${router.url}/${date}`, increments[date], router.token);
      });
    });
    await Promise.all(pings);

    // cleanup sockets (lambda executions seem to need this periodically)
    this._agents.http.destroy();
    this._agents.https.destroy();

    // return total counts incremented on each host
    return routers.map((r) => ({ dest: r.host, count }));
  }

  async increment(url, authToken, data) {
    try {
      await pingurl.post(url, authToken, data, this._timeout, this._timeoutWait, this._agents);
    } catch (err) {
      logger.warn(`PINGFAIL ${err}`, { url });
    }
  }
}
