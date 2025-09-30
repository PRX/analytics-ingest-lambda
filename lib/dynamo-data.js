import { promisify } from "node:util";
import { inflate as zinflate } from "node:zlib";
import log from "lambda-log";
import { toEpochMilliseconds, toISODateString } from "./timestamp";

const pinflate = promisify(zinflate);
const inflate = async (buff) => JSON.parse(await pinflate(buff));
const safeParseInt = (str) => {
  if (str === undefined) {
    return null;
  } else {
    const int = parseInt(str, 10);
    return Number.isNaN(int) ? str : int;
  }
};

/**
 * Utility class for inserted dynamodb redirect + segments-downloaded data
 */
export default class DynamoData {
  /**
   * Constructed from the results of dynamo.upsertRedirect, including both the
   * data we just set and what was previously in DDB.
   */
  constructor({ id, payload, segments, extras, result }) {
    this.id = id;
    this.setPayload = payload;
    this.setSegments = new Set((segments || []).map((s) => s.toString()));
    this.setExtras = extras;

    // dynamo returns "ALL_OLD" - so we know the previous values
    this.oldPayload = result?.Attributes?.payload ? result?.Attributes.payload.B : null;
    this.oldSegments = new Set(result?.Attributes?.segments ? result?.Attributes.segments.SS : []);
    this.oldExtras = result?.Attributes?.extras ? result?.Attributes.extras.S : null;
  }

  /**
   * Encode a record as timestamp + segment
   */
  static encodeSegment(rec) {
    const epochMs = toEpochMilliseconds(rec.timestamp || Date.now()).toString();
    if (rec.type === "segmentbytes") {
      return `${epochMs}.${rec.segment}`;
    } else {
      return epochMs;
    }
  }

  /**
   * Decode a segment back into the epoch + segmentIndex
   */
  static decodeSegment(str) {
    const parts = str.split(".");
    return [toEpochMilliseconds(safeParseInt(parts[0])), safeParseInt(parts[1])];
  }

  /**
   * Get the current payload + extras
   */
  async payload() {
    let payload = this.setPayload;
    if (!payload && this.oldPayload) {
      payload = await inflate(this.oldPayload);
    }

    // merge in extra data
    if (payload && this.setExtras) {
      Object.assign(payload, this.setExtras);
    } else if (payload && this.oldExtras) {
      try {
        Object.assign(payload, JSON.parse(this.oldExtras));
      } catch (error) {
        log.warn(`Error json parsing old extras`, { error, extras: this.oldExtras });
      }
    }

    // delete any stored 'msg', so it doesn't override our own lambda-logging
    if (payload?.msg) {
      delete payload.msg;
    }

    return payload || null;
  }

  /**
   * Calculate which segments need to be logged to kineses as postbytes
   */
  newSegments() {
    let segs = [];

    if (this.oldPayload) {
      segs = Array.from(this.setSegments.difference(this.oldSegments));
    } else if (this.setPayload) {
      segs = Array.from(this.setSegments.union(this.oldSegments));
    }

    // only return each segment once per UTC day
    const seen = {};
    return segs.sort().filter((s) => {
      const [epochMs, index] = DynamoData.decodeSegment(s);
      const utcDate = toISODateString(epochMs);
      if (seen[`${utcDate}.${index}`]) {
        return false;
      } else {
        seen[`${utcDate}.${index}`] = true;
        return true;
      }
    });
  }

  /**
   * Translate any new segments needing logging into postbyte records
   */
  async postBytes() {
    const segs = this.newSegments();
    if (!segs.length) {
      return [];
    }

    // group segment by: {utcDay: {segmentIndex: epochMs}}
    const allDecoded = segs.map((s) => DynamoData.decodeSegment(s));
    const allDays = allDecoded.reduce((acc, [ms, idx]) => {
      const day = toISODateString(ms);
      acc[day] ||= {};
      acc[day][idx] = ms;
      return acc;
    }, {});

    const type = "postbytes";
    const listenerEpisode = (this.id || "").split(".")[0];
    const digest = (this.id || "").split(".")[1];
    const payload = await this.payload();

    // create a record per day, to log back to kinesis
    return Object.keys(allDays).flatMap((day) => {
      const record = { type, listenerEpisode, digest, ...payload };

      // use min timestamp on overall record
      record.timestamp = Object.values(allDays[day]).sort()[0];

      // check if this should be an overall download
      if (allDays[day][null]) {
        record.download.timestamp = allDays[day][null];
      } else {
        delete record.download;
      }

      // filter impressions to just these segments
      record.impressions = record.impressions.flatMap((imp) => {
        if (allDays[day][imp.segment]) {
          return { ...imp, timestamp: allDays[day][imp.segment] };
        } else {
          return [];
        }
      });

      if (record.download || record.impressions.length) {
        return record;
      } else {
        return [];
      }
    });
  }
}
