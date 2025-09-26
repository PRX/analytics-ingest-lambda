import http from "node:http";
import https from "node:https";
import log from "lambda-log";
import { decodeRecords } from "./lib/decoder";
import * as pingurl from "./lib/pingurl";
import * as timestamp from "./lib/timestamp";
import * as urlutil from "./lib/urlutil";

/**
 * GET impression pingbacks and POST impression increments to dovetail router,
 * so it has up to date counts of what has served.
 */
export const handler = async (event) => {
  const records = await decodeRecords(event);

  // we only care about non-dup impressions here
  const imps = records
    .filter((r) => r.type === "postbytes")
    .flatMap((r) => (r.impressions || []).map((i) => ({ ...r, ...i })))
    .filter((r) => !r.isDuplicate);
  const pings = imps.flatMap((r) => (r.pings || []).map((pingUrl) => ({ ...r, pingUrl })));

  const info = { records: records.length, pingbacks: pings.length, increments: imps.length };
  log.info("Starting Pingbacks", info);

  // manually create agents, so we can cleanup sockets (TODO: is this still needed?)
  const agents = { http: new http.Agent(), https: new https.Agent() };

  // run pingbacks
  const pingRes = await Promise.all(pings.map((rec) => runPingback(rec, agents)));
  const pingCount = pingRes.filter((r) => r).length;
  const pingFail = pingRes.filter((r) => !r).length;

  // run increments
  const hosts = (process.env.DOVETAIL_ROUTER_HOSTS || "").split(",");
  const tokens = (process.env.DOVETAIL_ROUTER_API_TOKENS || "").split(",");
  const incrs = formatIncrements(imps);
  await Promise.all(hosts.map((h, i) => runIncrement(h, tokens[i], incrs, agents)));

  const info2 = {
    records: records.length,
    pingbacks: pingCount,
    pingfails: pingFail,
    increments: imps.length,
  };
  log.info("Finished Pingbacks", info2);
};

/**
 * Attempt to expand and ping a url template... and log everything for audit purposes
 */
export const runPingback = async (rec, agents) => {
  let url = rec.pingUrl;
  try {
    url = urlutil.expand(url, rec);
    await pingurl.ping(url, rec, null, null, agents);
    log.info("PINGED", { url });
    return true;
  } catch (err) {
    log.warn(`PINGFAIL ${err}`, { url });
    return false;
  }
};

/**
 * Group flight increments by date: {'2022-06-23': {'123': 1, '456': 2}}
 */
export const formatIncrements = (records) => {
  const increments = {};

  for (const rec of records) {
    const date = timestamp.toISODateString(rec.timestamp || 0);
    increments[date] = increments[date] || {};
    increments[date][rec.flightId] = (increments[date][rec.flightId] || 0) + 1;
  }

  return increments;
};

/**
 * POST increments to dovetail routers in each region
 */
export const runIncrement = async (host, token, incrs, agents) => {
  for (const date of Object.keys(incrs)) {
    const url = incrementUrl(host, date);
    try {
      await pingurl.post(url, incrs[date], token, null, null, agents);
    } catch (err) {
      log.warn(`INCRFAIL ${err}`, { url });
    }
  }

  return true;
};

export const incrementUrl = (host, date) => {
  if (host?.startsWith("http")) {
    return `${host}/api/v1/flight_increments/${date}`;
  } else if (host?.match(/localhost|127\.0\.0\.1/)) {
    return `http://${host}/api/v1/flight_increments/${date}`;
  } else {
    return `https://${host}/api/v1/flight_increments/${date}`;
  }
};
