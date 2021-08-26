const dynamo = require('../dynamo');
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
      } else {
        const segment = this.encodeSegment(rec);
        if (this.segments[key] && !this.segments[key].includes(segment)) {
          this.segments[key].push(segment);
        } else if (!this.segments[key]) {
          this.segments[key] = [segment];
        }
      }
    });
  }

  check(record) {
    if (record.type === 'antebytes' || record.type === 'antebytespreview') {
      return true;
    } else if (record.type === 'bytes' || record.type === 'segmentbytes') {
      return !record.isDuplicate;
    } else {
      return false;
    }
  }

  encodeKey({ listenerEpisode, digest, ...record }) {
    return [`${listenerEpisode}.${digest}`, record];
  }

  decodeKey(key, record) {
    const index = key.indexOf('.');
    const listenerEpisode = key.slice(0, index);
    const digest = key.slice(index + 1);
    return { listenerEpisode, digest, ...record };
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
    const segment = safeParseInt(parts[1] || OVERALL_DOWNLOAD);
    return [timestamp.toDateString(epoch), epoch, segment];
  }

  async insert() {
    const allKeys = Object.keys(this.payloads).concat(Object.keys(this.segments));
    const uniqueKeys = allKeys.filter((v, i, a) => a.indexOf(v) === i);

    if (uniqueKeys.length == 0) {
      return Promise.resolve([]);
    } else {
      const updates = uniqueKeys.map(k => [k, this.payloads[k], this.segments[k]]);

      logger.debug('Running DDB updates', { count: updates.length });
      const [successCount, failureCount, loggedCount] = await this.updateAll(updates);
      const output = [];
      if (successCount) {
        output.push({ dest: 'dynamodb', count: successCount });
      }
      if (loggedCount) {
        output.push({ dest: 'kinesis', count: loggedCount });
      }

      // just throw an error if anything failed. successful records have already
      // been recorded in DDB, so retrying them won't cause duplicates.
      if (failureCount) {
        const msg = `DDB retrying for ${failureCount}/${failureCount + successCount}`;
        logger.warn(msg, { ddb: 'retrying', count: failureCount });
        throw new Error(msg);
      }

      return output;
    }
  }

  // spin up N workers, and process the queue of updates
  async updateAll(updates, concurrency = 25) {
    console.log(`updateAll[START] - ${updates.length}`);
    let successCount = 0;
    let failureCount = 0;
    let loggedCount = 0;

    const client = await dynamo.client();
    const worker = async () => {
      let args;
      while ((args = updates.shift())) {
        console.log(`updateAll[UPDATE] - ${args[0]}`);
        try {
          const result = await dynamo.update.apply(this, args.concat([client]));
          successCount++;
          this.format(result).forEach(formatted => {
            logger.info('impression', formatted);
            loggedCount++;
          });
          console.log(
            `updateAll[SUCCESS] - ${args[0]} - ${successCount} success, ${failureCount} failed, ${loggedCount} logged, ${updates.length} remain`,
          );
        } catch (err) {
          logger.error(`DDB Error [${process.env.DDB_TABLE}]: ${err}`, { args });
          failureCount++;
          console.log(
            `updateAll[FAILURE] - ${args[0]} - ${successCount} success, ${failureCount} failed, ${loggedCount} logged, ${updates.length} remain`,
          );
        }
      }
    };

    const threads = Array(concurrency).fill(true);
    await Promise.all(threads.map(() => worker()));
    console.log(`END updateAll`);
    return [successCount, failureCount, loggedCount];
  }

  // create combined "postbytes" records, when we have both both the initial
  // redirect-data and the CDN segments-downloaded
  format([key, payload, segments]) {
    if (!payload || !segments) {
      return [];
    }

    // for each UTC day, construct a record and filter download/impressions
    const [todoSegments, todoTimestamps] = this.dedupSegments(segments);
    const formatted = Object.keys(todoSegments).map(day => {
      const record = this.decodeKey(key, payload);
      record.type = record.type === 'antebytespreview' ? 'postbytespreview' : 'postbytes';
      record.timestamp = todoTimestamps[day];

      // delete any stored 'msg', so it doesn't override our own lambda-logging
      delete record.msg;

      // check if this should be an overall download
      if (!todoSegments[day].includes(OVERALL_DOWNLOAD)) {
        delete record.download;
      }

      // filter segment impressions
      if (record.impressions) {
        const todoSegmentStrings = todoSegments[day].map(s => `${s}`);
        record.impressions = record.impressions.filter(i =>
          todoSegmentStrings.includes(`${i.segment}`),
        );
      }

      // only send if there's something to record
      if (record.download || (record.impressions && record.impressions.length > 0)) {
        return record;
      } else {
        return null;
      }
    });

    return formatted.filter(r => r);
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
