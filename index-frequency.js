import log from "lambda-log";
import { decodeRecords } from "./lib/decoder.js";
import * as dynamo from "./lib/dynamo.js";
import { toEpochMilliseconds } from "./lib/timestamp.js";

// Assume the longest we keep data is 30 days
// Use this to calculate ttl as updated_at + 30 days in seconds
const FREQUENCY_HORIZON_SECONDS = 30 * 24 * 60 * 60;

/**
 * Increment the DynamoDB frequency table, for campaigns with frequency caps
 */
export const handler = async (event) => {
  const records = await decodeRecords(event);

  // we only care about non-dup freq-capped impressions here
  const imps = records
    .filter((r) => r.type === "postbytes")
    .flatMap((r) => (r.impressions || []).map((i) => ({ ...r, ...i })))
    .filter((i) => i.frequency && !i.isDuplicate)
    .filter((i) => i.frequency.match(/[0-9]+:[0-9]+/));

  const info = { records: records.length, impressions: imps.length };
  log.info("Starting Frequency", info);

  // format non-expired impressions
  const frequencies = imps.map((i) => format(i)).filter((f) => isCurrent(f));

  // spin up workers and add/remove in parallel
  let removed = 0;
  const client = await dynamo.client();
  const [added, failures] = await dynamo.concurrently(25, frequencies, async (args) => {
    const result = await dynamo.addFrequency({ ...args, client });
    const timestamps = removeTimestamps(result, args);
    if (timestamps.length) {
      await dynamo.removeFrequency({ ...args, client, timestamps });
      removed += timestamps.length;
    }
  });

  const info2 = { records: records.length, added, removed, failures };
  log.info("Finished Frequency", info2);
};

/**
 * Format an impression (merged with the download record) for adding to DDB
 */
export function format({ frequency, listenerId, campaignId, timestamp }) {
  const days = (frequency || "").split(",").map((p) => parseInt(p.split(":")[1], 10));
  const secs = Math.max(...days) * 86400;
  const maxSeconds = Math.min(secs, FREQUENCY_HORIZON_SECONDS);
  return {
    listener: listenerId,
    campaign: campaignId,
    maxSeconds: maxSeconds,
    timestamp: toEpochMilliseconds(timestamp || Date.now()),
  };
}

/**
 * Determine if a frequency is current (its timestamp is greater than the horizon)
 */
export function isCurrent({ timestamp, maxSeconds }) {
  const minImpressionTime = Date.now() - maxSeconds * 1000;
  return toEpochMilliseconds(timestamp) > minImpressionTime;
}

/**
 * Remove old timestamps from the frequency table, when they've rolled off
 */
export function removeTimestamps(result, frequency) {
  const impressions = result?.Attributes?.impressions?.NS || [];
  const remove = impressions.filter((ts) => {
    return !isCurrent({ ...frequency, timestamp: ts });
  });

  // Only remove if the list is half or more of the total list, and greater than 10
  if (remove.length > 10 && remove.length * 2 >= impressions.length) {
    return remove;
  } else {
    return [];
  }
}
