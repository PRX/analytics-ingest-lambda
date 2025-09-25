import { promisify } from "node:util";
import { gzip } from "node:zlib";
import { jest } from "@jest/globals";
import log from "lambda-log";
import { decodeRecords } from "./decoder";

// some test records
const rec1 = { timestamp: 1, type: "one", thing: "one" };
const rec2 = { timestamp: 2, type: "two", thing: "\two" };
const rec3 = { timestamp: 3, type: "three", thing: "ThrEE" };
const rec4 = { timestamp: 4, type: "four", thing: "four" };

// lambda log line prefixes
const prefixOld = "2021-06-30T19:51:13.886Z\tee30dae3-f25c-4a22-84e8-8b7b378215fb\t";
const prefixNew = "2021-06-30T19:51:13.886Z\tee30dae3-f25c-4a22-84e8-8b7b378215fb\tINFO\t";

describe("decoder", () => {
  describe(".decodeRecords", () => {
    it("decodes base64 kinesis records", async () => {
      const data1 = Buffer.from(JSON.stringify(rec1), "utf-8").toString("base64");
      const data2 = Buffer.from(JSON.stringify(rec2), "utf-8").toString("base64");
      const event = {
        Records: [{ kinesis: { data: data1 } }, { kinesis: { data: data2 } }],
      };

      const recs = await decodeRecords(event);
      expect(recs).toEqual([rec1, rec2]);
    });

    it("decodes gzipped cloudwatch subscription filter events", async () => {
      const logEvents1 = [
        { message: JSON.stringify(rec1) },
        { message: JSON.stringify(rec2) },
        { message: JSON.stringify(rec3) },
      ];
      const logEvents2 = [{ message: JSON.stringify(rec4) }];
      const zipped1 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents1 }));
      const zipped2 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents2 }));
      const event = {
        Records: [
          { kinesis: { data: Buffer.from(zipped1).toString("base64") } },
          { kinesis: { data: Buffer.from(zipped2).toString("base64") } },
        ],
      };

      const recs = await decodeRecords(event);
      expect(recs).toEqual([rec1, rec2, rec3, rec4]);
    });

    it("decodes lambda format cloudwatch subscription filter events", async () => {
      const logEvents1 = [
        { message: `${prefixOld + JSON.stringify(rec1)}\n` },
        { message: `${prefixNew + JSON.stringify(rec2)}\n` },
        { message: prefixNew + JSON.stringify(rec3) },
      ];
      const logEvents2 = [{ message: prefixOld + JSON.stringify(rec4) }];
      const zipped1 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents1 }));
      const zipped2 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents2 }));
      const event = {
        Records: [
          { kinesis: { data: Buffer.from(zipped1).toString("base64") } },
          { kinesis: { data: Buffer.from(zipped2).toString("base64") } },
        ],
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
      const data1 = Buffer.from(JSON.stringify(rec1), "utf-8").toString("base64");

      const msg2 = [{ message: JSON.stringify(rec2) }, { message: JSON.stringify(rec3) }];
      const zip2 = await promisify(gzip)(JSON.stringify({ logEvents: msg2 }));
      const data2 = Buffer.from(zip2).toString("base64");

      const msg3 = [{ message: `${prefixNew + JSON.stringify(rec4)}\n` }];
      const zip3 = await promisify(gzip)(JSON.stringify({ logEvents: msg3 }));
      const data3 = Buffer.from(zip3).toString("base64");
      const event = {
        Records: [
          { kinesis: { data: data1 } },
          { kinesis: { data: data2 } },
          { kinesis: { data: data3 } },
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
