import { jest } from "@jest/globals";
import log from "lambda-log";
import { buildJsonRecord, buildLambdaRecord, buildLogRecord } from "../test/support";
import { decodeRecords } from "./decoder";

// some test records
const rec1 = { timestamp: 1, type: "one", thing: "one" };
const rec2 = { timestamp: 2, type: "two", thing: "\two" };
const rec3 = { timestamp: 3, type: "three", thing: "ThrEE" };
const rec4 = { timestamp: 4, type: "four", thing: "four" };

describe("decoder", () => {
  describe(".decodeRecords", () => {
    it("decodes base64 kinesis records", async () => {
      const event = {
        Records: [await buildJsonRecord(rec1), await buildJsonRecord(rec2)],
      };

      const recs = await decodeRecords(event);
      expect(recs).toEqual([rec1, rec2]);
    });

    it("decodes gzipped cloudwatch subscription filter events", async () => {
      const event = {
        Records: [await buildLogRecord([rec1, rec2, rec3]), await buildLogRecord(rec4)],
      };

      const recs = await decodeRecords(event);
      expect(recs).toEqual([rec1, rec2, rec3, rec4]);
    });

    it("decodes lambda format cloudwatch subscription filter events", async () => {
      const event = {
        Records: [await buildLambdaRecord([rec1, rec2, rec3]), await buildLambdaRecord(rec4, true)],
      };

      const recs = await decodeRecords(event);
      expect(recs).toEqual([rec1, rec2, rec3, rec4]);
    });

    it("handles bad records", async () => {
      const event = {
        Records: [{ what: "ever" }, { kinesis: { data: "badstuff" } }],
      };
      jest.spyOn(log, "error").mockReturnValue();

      expect(await decodeRecords(event)).toEqual([]);
      expect(log.error.mock.calls.length).toEqual(2);
      expect(log.error.mock.calls[0][0]).toMatch(/invalid kinesis record/i);
      expect(log.error.mock.calls[0][1]).toEqual({ record: { what: "ever" } });
      expect(log.error.mock.calls[1][0]).toMatch(/invalid kinesis record/i);
      expect(log.error.mock.calls[1][1]).toEqual({ record: { kinesis: { data: "badstuff" } } });
    });

    it("handles bad events", async () => {
      jest.spyOn(log, "error").mockReturnValue();

      expect(await decodeRecords()).toEqual([]);
      expect(log.error.mock.calls.length).toEqual(1);
      expect(log.error.mock.calls[0][0]).toMatch(/invalid event input/i);

      expect(await decodeRecords({ bad: "stuff" })).toEqual([]);
      expect(log.error.mock.calls.length).toEqual(2);
      expect(log.error.mock.calls[1][0]).toMatch(/invalid event input/i);
    });

    it("applies timestamp filters", async () => {
      const event = {
        Records: [
          await buildJsonRecord(rec1),
          await buildLogRecord(rec2),
          await buildLambdaRecord([rec3, rec4]),
        ],
      };

      expect(await decodeRecords(event)).toEqual([rec1, rec2, rec3, rec4]);

      process.env.PROCESS_AFTER = 2;
      expect(await decodeRecords(event)).toEqual([rec3, rec4]);

      process.env.PROCESS_AFTER = null;
      process.env.PROCESS_UNTIL = 3;
      expect(await decodeRecords(event)).toEqual([rec1, rec2, rec3]);

      process.env.PROCESS_AFTER = 1;
      expect(await decodeRecords(event)).toEqual([rec2, rec3]);
    });
  });
});
