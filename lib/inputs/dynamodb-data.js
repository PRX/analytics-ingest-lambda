const dynamo = require('../dynamo');
const kinesis = require('../kinesis');
const logger = require('../logger');
const timestamp = require('../timestamp');
const safeParseInt = str => {
  const int = parseInt(str, 10);
  return isNaN(int) ? str : int;
};
const OVERALL_DOWNLOAD = 'DOWNLOAD';

/**
 * Cache redirect-data and segment-bytes-downloaded-data into DynamoDB. ONLY
 * after both of those are present (there's a race condition!) do we throw the
 * redirect-data back onto kinesis to generate the real downloads/impressions.
 */
module.exports = class DynamodbData {
  constructor(records) {
    this._records = (records || []).filter(rec => this.check(rec));

    // organize into payloads and segments, grouping by key
    this.payloads = {};
    this.segments = {};
    this._records.forEach(raw => {
      const [key, rec] = this.encodeKey(raw);
      if (rec.type === 'antebytes' || rec.type === 'antebytespreview') {
        this.payloads[key] = rec;
      } else if (this.segments[key]) {
        this.segments[key].push(this.encodeSegment(rec));
      } else {
        this.segments[key] = [this.encodeSegment(rec)];
      }
    });
  }

  check(record) {
    return ['antebytes', 'antebytespreview', 'bytes', 'segmentbytes'].includes(record.type);
  }

  encodeKey({ listenerEpisode, digest, ...record }) {
    return [`${listenerEpisode}.${digest}`, record];
  }

  decodeKey(key, record) {
    const index = key.indexOf('.');
    record.listenerEpisode = key.slice(0, index);
    record.digest = key.slice(index + 1);
    return JSON.parse(JSON.stringify(record));
  }

  encodeSegment(rec) {
    const epochMs = (rec.timestamp || Date.now()).toString();
    if (rec.type === 'segmentbytes') {
      return `${epochMs}.${rec.segment}`;
    } else {
      return epochMs;
    }
  }

  decodeSegment(str) {
    const parts = str.split('.');
    const epoch = safeParseInt(parts[0]);
    const segment = safeParseInt(parts[1]) || OVERALL_DOWNLOAD;
    return [timestamp.toDateString(epoch), epoch, segment];
  }

  async insert() {
    const allKeys = Object.keys(this.payloads).concat(Object.keys(this.segments));
    const uniqueKeys = allKeys.filter((v, i, a) => a.indexOf(v) === i);

    if (uniqueKeys.length == 0) {
      return Promise.resolve([]);
    } else {
      const updates = uniqueKeys.map(k => [k, this.payloads[k], this.segments[k]]);
      const result = await dynamo.updateAll(updates);
      const output = [];

      // send "new" segments onwards to kinesis
      const formatted = result.success.map(r => this.format(r)).flat();
      if (formatted.length) {
        const count = await kinesis.put(formatted);
        output.push({ count, dest: `kinesis:${kinesis.stream()}` });
      }

      // just throw an error if anything failed. successful records have already
      // been recorded in DDB, so retrying them won't cause duplicates.
      if (result.failures.length > 0) {
        const fails = result.failures.length;
        const total = result.failures.length + result.success.length;
        const msg = `DDB retrying for ${fails}/${total}`;
        logger.warn(msg, { ddb: 'retrying', count: fails });
        throw new Error(msg);
      }

      return output;
    }
  }

  // create combined "postbytes" records, when we have both both the initial
  // redirect-data and the CDN segments-downloaded
  format([key, payload, segments]) {
    if (!payload || !segments) {
      return [];
    }

    // for each UTC day, construct a record and filter download/impressions
    const [todoSegments, todoTimestamps] = this.dedupSegments(segments);
    return Object.keys(todoSegments).map(day => {
      const record = this.decodeKey(key, payload);
      record.type = record.type === 'antebytespreview' ? 'postbytespreview' : 'postbytes';
      record.timestamp = todoTimestamps[day];

      // check if this should be an overall download
      if (!todoSegments[day].includes(OVERALL_DOWNLOAD)) {
        delete record.download;
      }

      // filter segment impressions
      if (record.impressions) {
        record.impressions = record.impressions.filter(i =>
          todoSegments[day].find(s => `${i.segment}` === `${s}`),
        );
      }

      return record;
    });
  }

  // we may have already processed a UTC-date + segment, and recieve newer
  // timestamps for that same day. dedup them so you can't get multiple
  // downloads/impressions on the same UTC day.
  dedupSegments(segments) {
    const done = {};
    Object.keys(segments).forEach(s => {
      if (segments[s] === false) {
        const [day, , segment] = this.decodeSegment(s);
        done[`${day}-${segment}`] = true;
      }
    });

    // find non-dup segments and mark latest timestamp per day
    const todoSegments = {};
    const todoTimestamps = {};
    Object.keys(segments).forEach(s => {
      const [day, timestamp, segment] = this.decodeSegment(s);
      if (segments[s] === true && !done[`${day}-${segment}`]) {
        todoSegments[day] = todoSegments[day] || [];
        todoSegments[day].push(segment);
        if (!todoTimestamps[day] || timestamp > todoTimestamps[day]) {
          todoTimestamps[day] = timestamp;
        }
      }
    });

    return [todoSegments, todoTimestamps];
  }
};
