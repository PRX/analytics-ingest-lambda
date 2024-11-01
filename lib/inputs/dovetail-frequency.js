'use strict';

const dynamo = require('../dynamo');
const timestamp = require('../timestamp');
const assayer = require('../assayer');
const logger = require('../logger');

// Assume the longest we keep data is 30 days
// Use this to calculate ttl as updated_at + 30 days in seconds
const FREQUENCY_HORIZON_SECONDS = 30 * 24 * 60 * 60; 

/**
 * Send dovetail impressions to ddb for frequency capped flights
 */
module.exports = class DovetailFrequency {
  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return record.type === 'postbytes' &&
      (record.impressions || []).length > 0 &&
      this.checkImpressions(record.impressions);
  }

  checkImpressions(impressions) {
    return impressions.some(this.checkFrequency);
  }

  checkFrequency(impression) {
    if (impression === undefined ||
        impression.frequency === undefined ||
        impression.frequency === null) {
      return false;
    } else {
      const freq = String(impression.frequency).trim();
      // a rough sanity check for if this is a real frequency value
      // it could probably be a regular expression
      return freq.length > 0 && freq.includes(":");
    }
  }

  tableName() {
    return process.env.DDB_FREQUENCY_TABLE;
  }

  async insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    }

    let updates = [];
    await this.eachImpression(async (rec, imp) => {
      const { isDuplicate } = await assayer.testImpression(rec, imp);
      if (!isDuplicate && this.checkFrequency(imp)) {
        logger.debug('Record that impression!', { rec: rec, imp: imp });
        const data = this.format(rec, imp);
        if (data.timestamp > this.minImpressionTime(data)) {
          updates.push(data);
        }
      }
    });

    logger.debug('Running DovetailFrequency updates', { count: updates.length });
    const [successCount, deleteCount, failureCount] = await this.updateAll(updates);
    const output = [];
    output.push({ dest: 'updates', count: successCount });
    output.push({ dest: 'deletes', count: deleteCount });

    // just throw an error if anything failed. successful records have already
    // been recorded in DDB, so retrying them won't cause duplicates.
    if (failureCount) {
      const msg = `DovetailFrequency retrying for ${failureCount}/${failureCount + successCount}`;
      logger.warn(msg, { ddb: 'retrying', count: failureCount });
      const err = new Error(msg);
      err.skipLogging = true;
      throw err;
    }

    return output;
  }

  // spin up N workers, and process the queue of updates
  async updateAll(updates, concurrency = 25) {
    let successCount = 0;
    let deleteCount = 0;
    let failureCount = 0;

    const client = await dynamo.client();
    const worker = async () => {
      let args;
      while ((args = updates.shift())) {
        try {
          const params = this.updateParams(args);
          const result = await dynamo.updateItemPromise(params, client);

          const toDelete = this.evictImpressions(args, result);
          if (toDelete.length > 0) {
            const deleteParams = this.deleteParams(args, toDelete);
            const deleteResult = await dynamo.updateItemPromise(deleteParams, client);
            deleteCount++;
          }

          successCount++;
        } catch (err) {
          if (err.name === 'ProvisionedThroughputExceededException') {
            logger.warn(`DDB throughput exceeded [${process.env.DDB_FREQUENCY_TABLE}]: ${err}`, { args });
          } else {
            logger.error(`DDB Error [${process.env.DDB_FREQUENCY_TABLE}]: ${err}`, { args });
          }
          failureCount++;
        }
      }
    };

    const threads = Array(concurrency).fill(true);
    await Promise.all(threads.map(() => worker()));
    return [successCount, deleteCount, failureCount];
  }

  minImpressionTime(data) {
    return Math.floor((new Date().getTime() / 1000) - data.maxSeconds);
  }

  evictImpressions(data, result) {
    let toEvict = [];
    if (result.Attributes && result.Attributes.impressions) {
      const impressions = result.Attributes.impressions.NS || [];
      const evictTime = this.minImpressionTime(data);
      const evictList = impressions.filter((imp) => imp <= evictTime);

      // Only evict if the list is half or more of the total list, and greater than 10
      if (evictList.length > 10 && evictList.length * 2 >= impressions.length) {
        toEvict = evictList;
      }
    }
    return toEvict;
  }

  deleteParams(data, deleteArray) {
    return {
      TableName: this.tableName(),
      Key: {
        listener: { S: `${data.listener}` },
        campaign: { S: `${data.campaign}` },
      },
      ReturnValues: 'ALL_NEW',
      UpdateExpression: "DELETE impressions :td",
      ExpressionAttributeValues: {
        ":td": { "NS": deleteArray.map((td) => `${td}`) },
      },
    };
  }

  updateParams(data) {
    return {
      TableName: this.tableName(),
      Key: {
        listener: { S: `${data.listener}` },
        campaign: { S: `${data.campaign}` },
      },
      ReturnValues: 'ALL_OLD',
      UpdateExpression: "SET expiration = :ttl ADD impressions :ts",
      ExpressionAttributeValues: {
        ":ttl": { "N": `${(data.timestamp + data.maxSeconds)}` },
        ":ts": { "NS": [`${data.timestamp}`] },
      },
    };
  }
  
  format(record, imp) {
    let out = {
      listener: record.listenerId,
      campaign: imp.campaignId,
      maxSeconds: this.maxSeconds(imp.frequency),
      timestamp: timestamp.toEpochSeconds(record.timestamp || 0),
    };
    return out;
  }

  maxSeconds(frequency) {
    const days = frequency.split(",").map((p) => parseInt(p.split(":")[1], 10));
    const secs = Math.max(...days) * 86400;
    return Math.min(secs, FREQUENCY_HORIZON_SECONDS);
  }

  async eachImpression(handler) {
    await Promise.all(
      this._records.map(async rec => {
        await Promise.all(
          rec.impressions.map(async imp => {
            await handler(rec, imp);
          }),
        );
      }),
    );
  }
};