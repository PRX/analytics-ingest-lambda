import log from "lambda-log";
import { decodeRecords } from "./lib/decoder";
import * as dynamo from "./lib/dynamo";
import DynamoData from "./lib/dynamo-data";

/**
 * Cache redirect-data (from DTR) and segment-bytes-downloaded (from dovetail-counts-lambda)
 * into DynamoDB.
 *
 * ONLY after we have both for the same listener/episode/UTC-day, pass the information back
 * into kinesis as "counted" (postbytes), to generate the real downloads/impressions.
 */
export const handler = async (event) => {
  const records = await decodeRecords(event);

  // redirect data from DTR
  const antebytes = records.filter((r) => r.type === "antebytes");

  // overall downloads and segments from counts-lambda
  const bytes = records.filter((r) => r.type === "bytes" && !r.isDuplicate);
  const segmentbytes = records.filter((r) => r.type === "segmentbytes" && !r.isDuplicate);
  const inputs = antebytes.concat(bytes).concat(segmentbytes);

  const info = {
    records: records.length,
    antebytes: antebytes.length,
    bytes: bytes.length,
    segmentbytes: segmentbytes.length,
  };
  log.info("Starting DynamoDB", info);

  // group all records by listenerEpisode + digest, and format for upsert
  const grouped = Object.groupBy(inputs, (r) => `${r.listenerEpisode}.${r.digest}`);
  const formatted = Object.values(grouped).map((recs) => formatUpsert(recs));

  // spin up workers and upsert
  const client = await dynamo.client();
  const threads = Array(event.dynamoConcurrency || 25).fill(true);
  const counts = await Promise.all(threads.map(() => upsertAndLog(formatted, client)));

  const info2 = {
    records: records.length,
    upserts: counts.reduce((sum, [s, _f, _l]) => sum + s, 0),
    failures: counts.reduce((sum, [_s, f, _l]) => sum + f, 0),
    logged: counts.reduce((sum, [_s, _f, l]) => sum + l, 0),
  };

  // retry entire batch on failure (any successful upserts will just no-op next time)
  if (info2.failures > 0) {
    log.warn("Retrying DynamoDB", info2);
    throw new Error(`Retrying ${info2.failures} DynamoDB failures`);
  } else {
    log.info("Finished DynamoDB", info2);
  }
};

/**
 * Upsert to DDB, then log any newly seen downloads/impressions "postbyte" records
 */
export const upsertAndLog = async (upserts, client) => {
  let success = 0;
  let failure = 0;
  let logged = 0;

  let args = upserts.shift();
  while (args) {
    try {
      const data = await dynamo.upsertRedirect({ ...args, client });
      success++;
      for (const rec of await data.postBytes()) {
        log.info("impression", rec);
        logged++;
      }
    } catch (err) {
      if (err.name === "ProvisionedThroughputExceededException") {
        log.warn(`DDB throughput exceeded [${process.env.DDB_TABLE}]: ${err}`, { err, args });
      } else {
        log.error(`DDB Error [${process.env.DDB_TABLE}]: ${err}`, { err, args });
      }
      failure++;
    }
    args = upserts.shift();
  }

  return [success, failure, logged];
};

/**
 * Format records for dynamo.upsertRedirect, keying on listenerEpisode + digest
 */
export const formatUpsert = (records) => {
  return records.reduce((data, { listenerEpisode, digest, ...rec }) => {
    data.id = `${listenerEpisode}.${digest}`;

    // record redirect-data payload, and the epoch-ms of bytes-downloaded
    if (rec.type === "antebytes") {
      data.payload = rec;
    } else {
      data.segments ||= [];
      data.segments.push(DynamoData.encodeSegment(rec));
      if (rec.durations) {
        data.extras = { durations: rec.durations, types: rec.types || "" };
      }
    }

    // dedup segments
    data.segments = [...new Set(data.segments)];

    return data;
  }, {});
};
