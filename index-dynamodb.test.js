import { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { jest } from "@jest/globals";
import "aws-sdk-client-mock-jest";
import { mockClient } from "aws-sdk-client-mock";
import log from "lambda-log";
import * as index from "./index-dynamodb";
import { deflate, inflate } from "./lib/dynamo";
import { buildEvent } from "./test/support";
import testRecs from "./test/support/test-records";

describe("index-dynamodb", () => {
  let ddbCalls, ddbResults, infos;

  // record DDB calls, and return fake results
  beforeEach(() => {
    ddbCalls = [];
    ddbResults = {};
    mockClient(DynamoDBClient)
      .on(UpdateItemCommand)
      .callsFake((params) => {
        ddbCalls.push(params);
        return ddbResults[params.Key.id.S];
      });

    infos = [];
    jest.spyOn(log, "info").mockImplementation((msg, args) => infos.push([msg, args]));
  });

  describe(".handler", () => {
    it("upserts to dynamodb and console logs", async () => {
      const mockPayload = await deflate({
        type: "antebytes",
        any: "thing",
        download: {},
        impressions: [
          { segment: 0, pings: ["ping", "backs"] },
          { segment: 1, pings: ["ping", "backs"] },
          { segment: 2, pings: ["ping", "backs"] },
        ],
      });
      ddbResults["listener-episode-3.the-digest"] = {
        Attributes: { payload: { B: mockPayload } },
      };

      await index.handler(await buildEvent(testRecs));

      expect(infos.length).toEqual(3);
      expect(infos[0][0]).toEqual("Starting DynamoDB");
      expect(infos[0][1]).toEqual({ records: 7, antebytes: 2, bytes: 1, segmentbytes: 1 });
      expect(infos[1][0]).toEqual("impression");
      expect(infos[1][1]).toEqual({
        any: "thing",
        digest: "the-digest",
        download: { timestamp: 1539287413617 },
        impressions: [],
        listenerEpisode: "listener-episode-3",
        timestamp: 1539287413617,
        type: "postbytes",
      });
      expect(infos[2][0]).toEqual("Finished DynamoDB");
      expect(infos[2][1]).toEqual({ records: 7, upserts: 3, failures: 0, logged: 1 });

      expect(ddbCalls.length).toEqual(3);
      const sortedArgs = ddbCalls.sort((a, b) => (a.Key.id.S < b.Key.id.S ? -1 : 1));

      const keys = sortedArgs.map((a) => a.Key.id.S);
      expect(keys).toEqual([
        "listener-episode-3.the-digest",
        "listener-episode-4.the-digest",
        "listener-episode-dtrouter-1.the-digest",
      ]);

      const payloads = await Promise.all(
        sortedArgs.map(async (a) => {
          if (a.AttributeUpdates.payload) {
            return inflate(a.AttributeUpdates.payload.Value.B);
          }
        }),
      );
      expect(payloads[0]).toBeFalsy();
      expect(payloads[1].type).toEqual("antebytes");
      expect(payloads[1].any).toEqual("thing");
      expect(payloads[2].type).toEqual("antebytes");
      expect(payloads[2].time).toEqual("2020-02-02T13:43:22.255Z");

      const segments = sortedArgs.map((a) => {
        if (a.AttributeUpdates.segments) {
          return a.AttributeUpdates.segments.Value.SS;
        } else {
          return null;
        }
      });
      expect(segments[0]).toEqual(["1539287413617", "1539287527000.4"]);
      expect(segments[1]).toBeFalsy();
      expect(segments[2]).toBeFalsy();
      expect(segments[3]).toBeFalsy();
    });

    it("ignores unknown types", async () => {
      const recs = [
        { timestamp: 2000, type: "postbytes" },
        { timestamp: 2000, type: "whatev" },
      ];
      await index.handler(await buildEvent(recs));

      expect(ddbCalls.length).toEqual(0);
      expect(infos[0][1]).toEqual({ records: 2, antebytes: 0, bytes: 0, segmentbytes: 0 });
    });

    it("ignores duplicate bytes and segmentbytes records", async () => {
      const recs = [
        { timestamp: 1, isDuplicate: true, cause: "whatev", type: "antebytes" },
        { timestamp: 1, isDuplicate: true, cause: "whatev", type: "bytes" },
        { timestamp: 1, isDuplicate: true, cause: "whatev", type: "segmentbytes" },
      ];
      await index.handler(await buildEvent(recs));

      expect(ddbCalls.length).toEqual(1);
      expect(ddbCalls[0].AttributeUpdates.payload).toBeDefined();
      expect(ddbCalls[0].AttributeUpdates.segments).toBeUndefined();
      expect(infos[0][1]).toEqual({ records: 3, antebytes: 1, bytes: 0, segmentbytes: 0 });
    });

    it("throws an error to retry any ddb failures", async () => {
      jest.spyOn(log, "warn").mockReturnValue();
      jest.spyOn(log, "error").mockReturnValue();

      mockClient(DynamoDBClient).on(UpdateItemCommand).rejects(new Error("bad stuff"));

      const event = await buildEvent(testRecs);
      await expect(index.handler(event)).rejects.toThrow(/retrying 3 dynamodb failures/i);

      // logged error for each of the 3 upsert attempts
      expect(log.error.mock.calls.length).toEqual(3);
      expect(log.error.mock.calls[0][0]).toMatch(/ddb error .+ bad stuff/i);
      expect(log.error.mock.calls[1][0]).toMatch(/ddb error .+ bad stuff/i);
      expect(log.error.mock.calls[2][0]).toMatch(/ddb error .+ bad stuff/i);

      // one logged warning that the overall lambda is throwing/retrying
      expect(log.warn.mock.calls.length).toEqual(1);
      expect(log.warn.mock.calls[0][0]).toMatch(/retrying dynamodb/i);
      expect(log.warn.mock.calls[0][1]).toEqual({ records: 7, upserts: 0, failures: 3, logged: 0 });
    });

    it("only warns on throughput exceeded errors", async () => {
      jest.spyOn(log, "warn").mockReturnValue();
      jest.spyOn(log, "error").mockReturnValue();

      const err = new Error();
      err.name = "ProvisionedThroughputExceededException";
      mockClient(DynamoDBClient).on(UpdateItemCommand).rejects(err);

      const event = await buildEvent(testRecs);
      await expect(index.handler(event)).rejects.toThrow(/retrying 3 dynamodb failures/i);

      // downgraded to warnings
      expect(log.error.mock.calls.length).toEqual(0);
      expect(log.warn.mock.calls.length).toEqual(4);
      expect(log.warn.mock.calls[0][0]).toMatch(/ddb throughput exceeded/i);
      expect(log.warn.mock.calls[1][0]).toMatch(/ddb throughput exceeded/i);
      expect(log.warn.mock.calls[2][0]).toMatch(/ddb throughput exceeded/i);
      expect(log.warn.mock.calls[3][0]).toMatch(/retrying dynamodb/i);
      expect(log.warn.mock.calls[3][1]).toEqual({ records: 7, upserts: 0, failures: 3, logged: 0 });
    });

    it("limits concurrent dynamo upserts", async () => {
      jest.spyOn(log, "warn").mockReturnValue();
      jest.spyOn(log, "error").mockReturnValue();

      // stub promises as update is called
      const promises = [],
        resolvers = [],
        rejectors = [];
      mockClient(DynamoDBClient)
        .on(UpdateItemCommand)
        .callsFake(() => {
          const p = new Promise((res, rej) => {
            resolvers.push(res);
            rejectors.push(rej);
          });
          promises.push(p);
          return p;
        });

      const t = Array(10).fill();
      const recs = t.map((_, i) => ({ type: "bytes", timestamp: 1, listenerEpisode: `le${i}` }));
      const event = await buildEvent(recs);
      const handlerPromise = index.handler({ ...event, dynamoConcurrency: 5 });

      // to start, we should see 5 calls
      await new Promise((r) => setTimeout(r, 10));
      expect(promises.length).toEqual(5);

      // resolving any picks up new
      resolvers[0](0);
      resolvers[3](3);
      resolvers[4](4);
      await new Promise((r) => process.nextTick(r));
      expect(promises.length).toEqual(8);

      // as do errors
      rejectors[1](1);
      rejectors[5](5);
      await new Promise((r) => process.nextTick(r));
      expect(promises.length).toEqual(10);
      expect(log.error.mock.calls.length).toEqual(2);
      expect(log.error.mock.calls[0][0]).toMatch(/ddb error/i);
      expect(log.error.mock.calls[1][0]).toMatch(/ddb error/i);

      // finish up
      resolvers[2](2);
      resolvers[6](6);
      resolvers[7](7);
      resolvers[8](8);
      resolvers[9](9);

      await expect(handlerPromise).rejects.toThrow(/retrying 2 dynamodb failures/i);
      expect(log.warn.mock.calls.length).toEqual(1);
      expect(log.warn.mock.calls[0][0]).toMatch(/retrying dynamodb/i);
      expect(log.warn.mock.calls[0][1]).toEqual({
        records: 10,
        upserts: 8,
        failures: 2,
        logged: 0,
      });
    });
  });

  describe("#upsertAndLog", () => {});

  describe("#formatUpsert", () => {
    it("includes payloads, segments, and extras", () => {
      const listenerEpisode = "le1";
      const digest = "d1";
      const durations = { the: "durations" };
      const types = { the: "types" };
      const recs = [
        { listenerEpisode, digest, timestamp: 1, type: "antebytes", the: "payload" },
        { listenerEpisode, digest, timestamp: 2, type: "bytes", durations, types },
        { listenerEpisode, digest, timestamp: 3, type: "segmentbytes", segment: 0 },
        { listenerEpisode, digest, timestamp: 4, type: "segmentbytes", segment: 2 },
      ];
      expect(index.formatUpsert(recs)).toEqual({
        id: "le1.d1",
        payload: { timestamp: 1, type: "antebytes", the: "payload" },
        segments: ["2000", "3000.0", "4000.2"],
        extras: { durations, types },
      });
    });

    it("dedups segments", () => {
      const recs = [
        { listenerEpisode: "le1", digest: "d1", timestamp: 2000, type: "bytes" },
        { listenerEpisode: "le1", digest: "d1", timestamp: 2000, type: "bytes" },
        { listenerEpisode: "le1", digest: "d1", timestamp: 4000, type: "segmentbytes", segment: 2 },
        { listenerEpisode: "le1", digest: "d1", timestamp: 4000, type: "segmentbytes", segment: 2 },
      ];
      expect(index.formatUpsert(recs)).toEqual({
        id: "le1.d1",
        segments: ["2000000", "4000000.2"],
      });
    });
  });
});
