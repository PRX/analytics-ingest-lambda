import http from "node:http";
import https from "node:https";
import log from "lambda-log";
import { decodeRecords } from "./lib/decoder";
import * as pingurl from "./lib/pingurl";
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

  const info = { records: records.length, increments: imps.length, pingbacks: pings.length };
  log.info("Starting Pingbacks", info);

  // manually create agents, so we can cleanup sockets (TODO: is this still needed?)
  const agents = { http: new http.Agent(), https: new https.Agent() };

  let pingCount = 0;
  let pingFail = 0;
  await Promise.all(
    pings.map(async (rec) => {
      try {
        const url = urlutil.expand(rec.pingUrl, rec);
        await pingurl.ping(url, rec, null, null, agents);
        log.info("PINGED", { url });
        pingCount++;
      } catch (err) {
        log.warn(`PINGFAIL ${err}`, { url: rec.pingUrl });
        pingFail++;
      }
    }),
  );

  const info2 = { records: records.length, pingbacks: pingCount, pingfails: pingFail };
  log.info("Finished Pingbacks", info2);
};
