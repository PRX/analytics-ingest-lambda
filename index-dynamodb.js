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

  // spin up workers and upsert in parallel
  let logged = 0;
  const client = await dynamo.client();
  const [upserts, failures] = await dynamo.concurrently(25, formatted, async (args) => {
    const data = await dynamo.upsertRedirect({ ...args, client });
    for (const rec of await data.postBytes()) {
      log.info("impression", rec);
      logged++;
    }
  });

  // retry entire batch on failure (any successful upserts will just no-op next time)
  const info2 = { records: records.length, upserts, failures, logged };
  if (failures > 0) {
    log.warn("Retrying DynamoDB", info2);
    throw new Error(`Retrying ${info2.failures} DynamoDB failures`);
  } else {
    log.info("Finished DynamoDB", info2);
  }
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
