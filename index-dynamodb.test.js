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
  describe(".handler", () => {
    it("upserts to dynamodb and console logs", async () => {
      jest.spyOn(log, "info").mockReturnValue();

      // return an old payload for just 1 update item call
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

      const ddbCalls = [];
      const ddbMock = mockClient(DynamoDBClient);
      ddbMock.on(UpdateItemCommand).callsFake((params) => {
        ddbCalls.push(params);
        if (params.Key.id.S === "listener-episode-3.the-digest") {
          return { Attributes: { payload: { B: mockPayload } } };
        }
      });

      const event = await buildEvent(testRecs);
      await index.handler(event);

      const infos = log.info.mock.calls;
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
  });
});
