import { GetItemCommand, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { jest } from "@jest/globals";
import log from "lambda-log";
import { v4 } from "uuid";
import { handler } from "../index-frequency";
import { client } from "../lib/dynamo";
import { buildEvent } from "./support";

describe("frequency integration", () => {
  beforeEach(() => {
    if (!process.env.DDB_FREQUENCY_TABLE) {
      throw new Error("You must set DDB_FREQUENCY_TABLE");
    }
    if (process.env.DDB_FREQUENCY_TABLE.match(/production|staging/)) {
      throw new Error("Not allowed to use prod/stag DDB_FREQUENCY_TABLE");
    }

    infos = [];
    jest.spyOn(log, "info").mockImplementation((msg, args) => infos.push([msg, args]));
  });

  it("increments campaign frequencies", async () => {
    const type = "postbytes";
    const listenerId = v4();
    const timestamp = Date.now();
    const recs = [
      {
        type,
        listenerId,
        timestamp,
        impressions: [
          { campaignId: 11, frequency: "1:7" },
          { campaignId: 22, frequency: "4:10" },
        ],
      },
      {
        type,
        listenerId,
        timestamp: timestamp + 1,

        impressions: [{ campaignId: 11, frequency: "1:7" }],
      },
      {
        type,
        listenerId,
        timestamp: timestamp + 1000,

        impressions: [{ campaignId: 11, frequency: "1:7" }],
      },
    ];

    await handler(await buildEvent(recs));
    expect(infos.length).toEqual(2);
    expect(infos[0][1]).toEqual({ records: 3, impressions: 4 });
    expect(infos[1][1]).toEqual({ records: 3, added: 4, failures: 0, removed: 0 });

    // lookup
    const ddb = await client();
    const cmd1 = new GetItemCommand({
      TableName: process.env.DDB_FREQUENCY_TABLE,
      Key: { listener: { S: listenerId }, campaign: { S: "11" } },
    });
    const res1 = await ddb.send(cmd1);
    const cmd2 = new GetItemCommand({
      TableName: process.env.DDB_FREQUENCY_TABLE,
      Key: { listener: { S: listenerId }, campaign: { S: "22" } },
    });
    const res2 = await ddb.send(cmd2);

    // check timestamps - (epoch milliseconds)
    expect(res1.Item.impressions.NS.sort()).toEqual([
      `${timestamp}`,
      `${timestamp + 1}`,
      `${timestamp + 1000}`,
    ]);
    expect(res2.Item.impressions.NS).toEqual([`${timestamp}`]);

    // expiration should be 7 and 10 days in the future (more or less)
    const exp7 = Math.floor(timestamp / 1000) + 7 * 86400;
    const exp10 = Math.floor(timestamp / 1000) + 10 * 86400;
    expect(parseInt(res1.Item.expiration.N, 10)).toBeGreaterThan(exp7 - 60);
    expect(parseInt(res1.Item.expiration.N, 10)).toBeLessThan(exp7 + 60);
    expect(parseInt(res2.Item.expiration.N, 10)).toBeGreaterThan(exp10 - 60);
    expect(parseInt(res2.Item.expiration.N, 10)).toBeLessThan(exp10 + 60);

    // add a bunch of old timestamps
    const times = Array(12)
      .fill(true)
      .map((_, i) => timestamp - 86400 * 1000 * 10 - i)
      .map((ts) => ts.toString());
    const cmd3 = new UpdateItemCommand({
      TableName: process.env.DDB_FREQUENCY_TABLE,
      Key: { listener: { S: listenerId }, campaign: { S: "11" } },
      UpdateExpression: "ADD impressions :ts",
      ExpressionAttributeValues: { ":ts": { NS: times } },
    });
    await ddb.send(cmd3);

    // add 1 more current timestamp - should remove old
    const impressions = [{ campaignId: 11, frequency: "1:7" }];
    const recs2 = [{ type, listenerId, timestamp, impressions }];
    await handler(await buildEvent(recs2));
    expect(infos.length).toEqual(4);
    expect(infos[2][1]).toEqual({ records: 1, impressions: 1 });
    expect(infos[3][1]).toEqual({ records: 1, added: 1, failures: 0, removed: 12 });

    // lookup again - deduped timestamps will result in 3
    const res3 = await ddb.send(cmd1);
    expect(res3.Item.impressions.NS.sort()).toEqual(res1.Item.impressions.NS.sort());
  });
});
