import { promisify } from "node:util";
import { gzip } from "node:zlib";
import { jest } from "@jest/globals";
import log from "lambda-log";
import { getRecordsFromEvent } from "./get-records";

describe("get-records", () => {
  describe(".getRecordsFromEvent", () => {
    it("decodes base64 kinesis records", async () => {
      const data1 = Buffer.from(JSON.stringify({ thing: "one" }), "utf-8").toString("base64");
      const data2 = Buffer.from(JSON.stringify({ thing: "\two" }), "utf-8").toString("base64");
      const event = {
        Records: [{ kinesis: { data: data1 } }, { kinesis: { data: data2 } }],
      };

      const recs = await getRecordsFromEvent(event);
      expect(recs).toEqual([{ thing: "one" }, { thing: "\two" }]);
    });

    it("decodes gzipped cloudwatch subscription filter events", async () => {
      const logEvents1 = [
        { message: JSON.stringify({ thing: "one" }) },
        { message: JSON.stringify({ thing: "two" }) },
        { message: JSON.stringify({ thing: "three" }) },
      ];
      const logEvents2 = [{ message: JSON.stringify({ thing: "four" }) }];
      const zipped1 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents1 }));
      const zipped2 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents2 }));
      const event = {
        Records: [
          { kinesis: { data: Buffer.from(zipped1).toString("base64") } },
          { kinesis: { data: Buffer.from(zipped2).toString("base64") } },
        ],
      };

      const recs = await getRecordsFromEvent(event);
      expect(recs).toEqual([
        { thing: "one" },
        { thing: "two" },
        { thing: "three" },
        { thing: "four" },
      ]);
    });

    it("decodes lambda format cloudwatch subscription filter events", async () => {
      const prefixOld = "2021-06-30T19:51:13.886Z\tee30dae3-f25c-4a22-84e8-8b7b378215fb\t";
      const prefixNew = "2021-06-30T19:51:13.886Z\tee30dae3-f25c-4a22-84e8-8b7b378215fb\tINFO\t";
      const logEvents1 = [
        { message: `${prefixOld + JSON.stringify({ thing: "one" })}\n` },
        { message: `${prefixNew + JSON.stringify({ thing: "two" })}\n` },
        { message: prefixNew + JSON.stringify({ thing: "th\tree" }) },
      ];
      const logEvents2 = [{ message: prefixOld + JSON.stringify({ thing: "four" }) }];
      const zipped1 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents1 }));
      const zipped2 = await promisify(gzip)(JSON.stringify({ logEvents: logEvents2 }));
      const event = {
        Records: [
          { kinesis: { data: Buffer.from(zipped1).toString("base64") } },
          { kinesis: { data: Buffer.from(zipped2).toString("base64") } },
        ],
      };

      const recs = await getRecordsFromEvent(event);
      expect(recs).toEqual([
        { thing: "one" },
        { thing: "two" },
        { thing: "th\tree" },
        { thing: "four" },
      ]);
    });

    it("handles bad records", async () => {
      const event = {
        Records: [{ what: "ever" }, { kinesis: { data: "badstuff" } }],
      };
      jest.spyOn(log, "error").mockReturnValue();

      expect(await getRecordsFromEvent(event)).toEqual([null, null]);
      expect(log.error.mock.calls.length).toEqual(2);
      expect(log.error.mock.calls[0][0]).toMatch(/invalid record input/i);
      expect(log.error.mock.calls[0][1]).toEqual({ record: '{"what":"ever"}' });
      expect(log.error.mock.calls[1][0]).toMatch(/invalid record input/i);
      expect(log.error.mock.calls[1][1]).toEqual({ record: '{"kinesis":{"data":"badstuff"}}' });
    });
  });
});
