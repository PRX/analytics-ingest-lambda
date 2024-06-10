'use strict';

const dynamo = require('../dynamo');
const timestamp = require('../timestamp');
const assayer = require('../assayer');
const logger = require('../logger');

// Assume the longest we keep data is 30 days
// Use this to calculate ttl as updated_at + 30 days in seconds
const FREQUENCY_HORIZON_SECONDS = 30 * 24 * 60 * 60; 

/**
 * Send dovetail impressions to bigquery
 */
module.exports = class DovetailFrequency {
  constructor(records) {
    this._records = (records || []).filter(r => this.check(r));
  }

  check(record) {
    return record.type === 'postbytes' && (record.impressions || []).length > 0;
  }

  tableName() {
    return process.env.DDB_FREQUENCY_TABLE || "DovetailListenerFrequency";
  }

  async insert() {
    if (this._records.length == 0) {
      return Promise.resolve([]);
    }

    let updates = [];
    await this.eachImpression(async (rec, imp) => {
      const { isDuplicate } = await assayer.testImpression(rec, imp);
      if (!isDuplicate) {
        logger.debug('Log that impression!', { rec: rec, imp: imp });
        const formattedRec = await this.format(rec, imp);
        updates.push(formattedRec);
      }
    });

    const [successCount, failureCount] = await this.updateAll(updates);

    // just throw an error if anything failed. successful records have already
    // been recorded in DDB, so retrying them won't cause duplicates.
    if (failureCount) {
      const msg = `DDB retrying for ${failureCount}/${failureCount + successCount}`;
      logger.warn(msg, { ddb: 'retrying', count: failureCount });
      const err = new Error(msg);
      err.skipLogging = true;
      throw err;
    }
    return successCount;
  }

  // spin up N workers, and process the queue of updates
  async updateAll(updates, concurrency = 25) {
    let successCount = 0;
    let failureCount = 0;

    const client = await dynamo.client();
    const worker = async () => {
      let args;
      while ((args = updates.shift())) {
        try {
          const params = {
            TableName: this.tableName(),
            Key: {
              listener: { S: `${args.listener}` },
              campaign: { S: `${args.campaign}` },
            },
            ReturnValues: 'ALL_OLD',
            UpdateExpression: "SET expiration = :ttl ADD impressions :ts",
            ExpressionAttributeValues: {
              ":ttl": { "N": `${(args.timestamp + FREQUENCY_HORIZON_SECONDS)}` },
              ":ts": { "NS": [`${args.timestamp}`] },
            },
          };

          const result = await client.updateItem(params).promise();

          const evict = Math.floor((new Date().getTime() - 30 * 24 * 60 * 60 * 1000) / 1000);
          if (result.Attributes && result.Attributes.impressions) {
            const toDelete = result.Attributes.impressions.NS.filter((imp) => imp <= evict);
            if (toDelete.length > 0) {
              const deleteParams = {
                TableName: this.tableName(),
                Key: {
                  listener: { S: `${args.listener}` },
                  campaign: { S: `${args.campaign}` },
                },
                ReturnValues: 'ALL_NEW',
                UpdateExpression: "DELETE impressions :td",
                ExpressionAttributeValues: {
                  ":td": { "NS": toDelete.map((td) => `${td}`) },
                },
              };
              const deleteResult = await client.updateItem(deleteParams).promise();
              console.log("toDelete", toDelete);
              console.log("deleteResult", deleteResult);
            }
          }

          successCount++;
        } catch (err) {
          if (err.name === 'ProvisionedThroughputExceededException') {
            logger.warn(`DDB throughput exceeded [${process.env.DDB_TABLE}]: ${err}`, { args });
          } else {
            logger.error(`DDB Error [${process.env.DDB_TABLE}]: ${err}`, { args });
          }
          failureCount++;
        }
      }
    };

    const threads = Array(concurrency).fill(true);
    await Promise.all(threads.map(() => worker()));
    return [successCount, failureCount];
  }
  
  async format(record, imp) {
    let out = {
      listener: record.listenerId,
      campaign: imp.campaignId,
      timestamp: timestamp.toEpochSeconds(record.timestamp || 0),
    };
    return out;
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