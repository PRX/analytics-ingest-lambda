const zlib = require("node:zlib");
const { promisify } = require("node:util");
const gzip = promisify(zlib.gzip);

const { getRecordsFromEvent } = require("../lib/get-records");
const _logger = require("../lib/logger");

describe("get-records", () => {
  it("decodes base64 kinesis records", async () => {
    const data1 = Buffer.from(JSON.stringify({ thing: "one" }), "utf-8").toString("base64");
    const data2 = Buffer.from(JSON.stringify({ thing: "\two" }), "utf-8").toString("base64");
    const event = {
      Records: [{ kinesis: { data: data1 } }, { kinesis: { data: data2 } }],
    };

    const recs = await getRecordsFromEvent(event);
    expect(recs).to.eql([{ thing: "one" }, { thing: "\two" }]);
  });

  it("decodes gzipped cloudwatch subscription filter events", async () => {
    const logEvents1 = [
      { message: JSON.stringify({ thing: "one" }) },
      { message: JSON.stringify({ thing: "two" }) },
      { message: JSON.stringify({ thing: "three" }) },
    ];
    const logEvents2 = [{ message: JSON.stringify({ thing: "four" }) }];

    const zipped1 = await gzip(JSON.stringify({ logEvents: logEvents1 }));
    const zipped2 = await gzip(JSON.stringify({ logEvents: logEvents2 }));

    const event = {
      Records: [
        { kinesis: { data: Buffer.from(zipped1).toString("base64") } },
        { kinesis: { data: Buffer.from(zipped2).toString("base64") } },
      ],
    };

    const recs = await getRecordsFromEvent(event);
    expect(recs).to.eql([
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

    const zipped1 = await gzip(JSON.stringify({ logEvents: logEvents1 }));
    const zipped2 = await gzip(JSON.stringify({ logEvents: logEvents2 }));

    const event = {
      Records: [
        { kinesis: { data: Buffer.from(zipped1).toString("base64") } },
        { kinesis: { data: Buffer.from(zipped2).toString("base64") } },
      ],
    };

    const recs = await getRecordsFromEvent(event);
    expect(recs).to.eql([
      { thing: "one" },
      { thing: "two" },
      { thing: "th\tree" },
      { thing: "four" },
    ]);
  });
});
