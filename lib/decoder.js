import { promisify } from "node:util";
import { gunzip } from "node:zlib";
import log from "lambda-log";
import { toEpochMilliseconds } from "./timestamp.js";

const gunzipPromise = promisify(gunzip);

/**
 * Parse lambda event kinesis records, and filter/log invalid
 */
export async function decodeRecords(event) {
  if (!event || !event.Records) {
    log.error("Invalid event input", { event });
    return [];
  }

  // decode multiple flavors of kinesis records
  const recs = await Promise.all(event.Records.map((r) => decodeJson(r) || decodeLines(r)));

  // remove invalid records and optionally apply time windowing
  return recs.flat().filter((rec) => {
    if (rec?.timestamp && rec.type) {
      if (process.env.PROCESS_AFTER || process.env.PROCESS_UNTIL) {
        const time = toEpochMilliseconds(rec.timestamp);
        const after = toEpochMilliseconds(parseInt(process.env.PROCESS_AFTER, 10) || 0);
        const until = toEpochMilliseconds(parseInt(process.env.PROCESS_UNTIL, 10) || Infinity);
        return time > after && time <= until;
      } else {
        return true;
      }
    } else {
      log.error("Invalid kinesis record", { record: rec });
      return false;
    }
  });
}

/**
 * Records PUT directly to the kinesis API may just be base64 json
 */
export function decodeJson(rec) {
  try {
    return JSON.parse(Buffer.from(rec.kinesis.data, "base64").toString("utf-8"));
  } catch (_err) {
    return null;
  }
}

/**
 * Cloudwatch log subscription filters will be base64+gzipped
 */
export async function decodeLines(rec) {
  try {
    const buffer = Buffer.from(rec.kinesis.data, "base64");
    const unzipped = await gunzipPromise(buffer);
    return JSON.parse(unzipped).logEvents.map((le) => decodeLine(le.message));
  } catch (_err) {
    return rec;
  }
}

/**
 * Actual CW log lines may be:
 *   "<json>" (ECS tasks)
 *   "<time>\t<guid>\t<json>" (old Lambdas)
 *   "<time>\t<guid>\t<level>\t<json>" (newer Lambdas)
 */
export function decodeLine(line) {
  try {
    const parts = line.split("\t");
    if (parts.length === 3 || parts.length === 4) {
      return JSON.parse(parts[parts.length - 1]);
    } else {
      return JSON.parse(line);
    }
  } catch (_err) {
    return line;
  }
}
