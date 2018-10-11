'use strict';

const timestamp = require('../timestamp');
const bigquery = require('../bigquery');

/**
 * Send dovetail downloads to bigquery
 */
module.exports = class DovetailDownloadBytes {

  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return record.type === 'bytes' || record.type === 'segmentbytes';
  }

  insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    } else {
      let tables = {}
      this._records.forEach(r => {
        if (r.type === 'bytes') {
          if (!tables.dt_download_bytes) {
            tables.dt_download_bytes = []
          }
          tables.dt_download_bytes.push(this.formatDownload(r))
        } else {
          if (!tables.dt_impression_bytes) {
            tables.dt_impression_bytes = []
          }
          tables.dt_impression_bytes.push(this.formatImpression(r))
        }
      })

      // run per-table inserts in parallel
      return Promise.all(Object.keys(tables).map(t => {
        return bigquery.insert(t, tables[t]).then(num => {
          return {count: num, dest: t};
        });
      }));
    }
  }

  formatDownload(record) {
    return {
      insertId: `bytes-${record.request_uuid}`,
      json: {
        timestamp: timestamp.toEpochSeconds(record.timestamp || 0),
        request_uuid: record.request_uuid,
        bytes: record.bytes_downloaded,
        seconds: record.seconds_downloaded,
        percent: record.percent_downloaded
      }
    };
  }

  formatImpression(record) {
    let formatted = this.formatDownload(record)
    formatted.insertId = `${formatted.insertId}-${record.segment_index}`;
    formatted.json.segment_index = record.segment_index;
    return formatted;
  }

}
