import { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { jest } from "@jest/globals";
import log from "lambda-log";
import * as index from "./index-frequency";
import { buildEvent } from "./test/support";
import "aws-sdk-client-mock-jest";
import { mockClient } from "aws-sdk-client-mock";

const days = (d) => d * 24 * 60 * 60;
const epoch = () => Math.floor(Date.now() / 1000);
const offset = (e, d) => (d ? e + days(d) : epoch() + days(e));

describe("index-frequency", () => {
  let ddbCalls, ddbResults, infos;

  // record DDB calls, and return fake results
  beforeEach(() => {
    infos = [];
    jest.spyOn(log, "info").mockImplementation((msg, args) => infos.push([msg, args]));
  });

  describe(".handler", () => {
    it("adds and removes frequency capped impressions", async () => {
      ddbCalls = [];
      ddbResults = {};
      mockClient(DynamoDBClient)
        .on(UpdateItemCommand)
        .callsFake((params) => {
          ddbCalls.push(params);
          return ddbResults[`${params.Key.listener.S}.${params.Key.campaign.S}`];
        });

      const epochMs = Date.now();
      const epoch = Math.floor(epochMs / 1000);

      // return some existing/real-old timestamps from DDB response
      ddbResults["l1.11"] = {
        Attributes: { impressions: { NS: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] } },
      };
      ddbResults["l2.33"] = {
        Attributes: { impressions: { NS: [1, 2, 3, 4, 5, 6, 7, 8, 9, epoch - 1, epoch] } },
      };

      // postbytes with frequency caps
      const recs = [
        {
          type: "postbytes",
          timestamp: epochMs,
          listenerId: "l1",
          impressions: [{ campaignId: 11, frequency: "1:7" }],
        },
        {
          type: "postbytes",
          timestamp: epoch,
          listenerId: "l2",
          impressions: [
            { campaignId: 22, frequency: "3:10" },
            { campaignId: 33, frequency: "1:1" },
          ],
        },
      ];
      await index.handler(await buildEvent(recs));

      expect(infos.length).toEqual(2);
      expect(infos[0][0]).toEqual("Starting Frequency");
      expect(infos[0][1]).toEqual({ records: 2, impressions: 3 });
      expect(infos[1][0]).toEqual("Finished Frequency");
      expect(infos[1][1]).toEqual({ records: 2, added: 3, failures: 0, removed: 11 });

      expect(ddbCalls.length).toEqual(4);
      expect(ddbCalls[0].Key).toEqual({ listener: { S: "l1" }, campaign: { S: "11" } });
      expect(ddbCalls[0].ExpressionAttributeValues[":ttl"].N).toEqual(`${offset(epoch, 7)}`);
      expect(ddbCalls[0].ExpressionAttributeValues[":ts"].NS).toEqual([`${epochMs}`]);
      expect(ddbCalls[1].Key).toEqual({ listener: { S: "l2" }, campaign: { S: "22" } });
      expect(ddbCalls[1].ExpressionAttributeValues[":ttl"].N).toEqual(`${offset(epoch, 10)}`);
      expect(ddbCalls[1].ExpressionAttributeValues[":ts"].NS).toEqual([`${epoch * 1000}`]);
      expect(ddbCalls[2].Key).toEqual({ listener: { S: "l2" }, campaign: { S: "33" } });
      expect(ddbCalls[2].ExpressionAttributeValues[":ttl"].N).toEqual(`${offset(epoch, 1)}`);
      expect(ddbCalls[2].ExpressionAttributeValues[":ts"].NS).toEqual([`${epoch * 1000}`]);

      expect(ddbCalls[3].Key).toEqual({ listener: { S: "l1" }, campaign: { S: "11" } });
      expect(ddbCalls[3].ExpressionAttributeValues[":td"].NS).toEqual(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map((n) => n.toString()),
      );
    });

    it("ignores other types, dups, and non-frequencies", async () => {
      const mock = mockClient(DynamoDBClient).on(UpdateItemCommand).resolves({});
      const type = "postbytes";
      const timestamp = Date.now();
      const event = await buildEvent([
        // wrong type
        { type: "antebytes", timestamp, impressions: [{ frequency: "1:7" }] },
        // no frequency
        { type, timestamp, impressions: [{}] },
        // invalid frequency string
        { type, timestamp, impressions: [{ frequency: "1:" }] },
        // impression is a duplicate
        { type, timestamp, impressions: [{ frequency: "1:7", isDuplicate: true }] },
        // is expired (more than 7 days ago)
        { type, timestamp: offset(-8), impressions: [{ frequency: "1:7" }] },
      ]);

      await index.handler(event);
      expect(mock).not.toHaveReceivedAnyCommand();

      expect(infos.length).toEqual(2);
      expect(infos[0][0]).toEqual("Starting Frequency");
      expect(infos[0][1]).toEqual({ records: 5, impressions: 1 });
      expect(infos[1][0]).toEqual("Finished Frequency");
      expect(infos[1][1]).toEqual({ records: 5, added: 0, failures: 0, removed: 0 });
    });
  });

  describe(".format", () => {
    it("formats records for insert", () => {
      const timestamp = Date.now();
      const rec = { listenerId: "l1", campaignId: 22, frequency: "1:2", timestamp };
      expect(index.format(rec)).toEqual({
        listener: "l1",
        campaign: 22,
        maxSeconds: days(2),
        timestamp,
      });
    });

    it("calculates maxSeconds", () => {
      expect(index.format({ frequency: "1:1" }).maxSeconds).toEqual(days(1));
      expect(index.format({ frequency: "6:4" }).maxSeconds).toEqual(days(4));
      expect(index.format({ frequency: "31:30" }).maxSeconds).toEqual(days(30));

      // max is 30 days
      expect(index.format({ frequency: "1234:5678" }).maxSeconds).toEqual(days(30));
    });

    it("converts timestamps to milliseconds", () => {
      expect(index.format({ timestamp: 1 }).timestamp).toEqual(1000);
      expect(index.format({ timestamp: 1000 }).timestamp).toEqual(1000000);
      expect(index.format({ timestamp: 1000000000000 }).timestamp).toEqual(1000000000000);
    });
  });

  describe(".isCurrent", () => {
    it("checks for impression timestamps that expired", () => {
      const epochMs = Date.now();
      const epoch = Math.floor(epochMs / 1000);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epoch + 10 })).toEqual(true);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epoch })).toEqual(true);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epoch - 9 })).toEqual(true);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epoch - 10 })).toEqual(false);

      // also handles milliseconds
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epochMs + 10000 })).toEqual(true);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epochMs })).toEqual(true);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epochMs - 9900 })).toEqual(true);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: epochMs - 10000 })).toEqual(false);

      // also handles strings
      expect(index.isCurrent({ maxSeconds: 10, timestamp: `${epoch}` })).toEqual(true);
      expect(index.isCurrent({ maxSeconds: 10, timestamp: `${epoch - 10}` })).toEqual(false);
    });
  });

  describe(".removeTimestamps", () => {
    it("removes > 10 timestamps", () => {
      const epochMs = Date.now();
      const epoch = Math.floor(epochMs / 1000);

      const result = { Attributes: { impressions: { NS: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] } } };
      const frequency = { maxSeconds: 10 };
      expect(index.removeTimestamps(result, frequency)).toEqual([]);

      // more than 10, but these 4 are current
      result.Attributes.impressions.NS.push(epoch, epochMs, epoch - 9, epochMs - 9900);
      expect(index.removeTimestamps(result, frequency)).toEqual([]);

      // push a non-current timestamp
      result.Attributes.impressions.NS.push(epoch - 10);
      const remove = index.removeTimestamps(result, frequency);
      expect(remove).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, epoch - 10]);
    });

    it("only removes when more than half the list", () => {
      const epochMs = Date.now();
      const epoch = Math.floor(epochMs / 1000);

      const result = { Attributes: { impressions: { NS: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] } } };
      const frequency = { maxSeconds: 10 };
      expect(index.removeTimestamps(result, frequency).length).toEqual(11);

      // push 12 current timestamps (vs 11 expired)
      for (let i = 0; i < 12; i++) {
        result.Attributes.impressions.NS.push(epoch);
      }
      expect(index.removeTimestamps(result, frequency)).toEqual([]);

      // one more expired
      result.Attributes.impressions.NS.push(epochMs - 13000);
      expect(index.removeTimestamps(result, frequency).length).toEqual(12);
    });
  });
});
