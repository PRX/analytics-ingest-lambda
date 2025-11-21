import { jest } from "@jest/globals";
import log from "lambda-log";
import { v4 } from "uuid";
import { handler } from "../index-dynamodb";
import { buildEvent } from "./support";

describe("dynamodb integration", () => {
  beforeEach(() => {
    if (!process.env.DDB_TABLE) {
      throw new Error("You must set DDB_TABLE");
    }
    if (!process.env.DDB_TTL) {
      throw new Error("You must set DDB_TTL");
    }
    if (process.env.DDB_TABLE.match(/production|staging/)) {
      throw new Error("Not allowed to use prod/stag DDB_TABLE");
    }

    infos = [];
    jest.spyOn(log, "info").mockImplementation((msg, args) => infos.push([msg, args]));
  });

  it("inserts redirects then segments", async () => {
    const listenerEpisode = v4();
    const digest = "the-digest";
    const timestamp = Date.now();
    const requestUuid = v4();
    const recs = [
      {
        listenerEpisode,
        digest,
        timestamp,
        requestUuid,
        type: "antebytes",
        download: { the: "download" },
        impressions: [
          { isDuplicate: true, cause: "stuff", segment: 1, flightId: 88 },
          { segment: 3, flightId: 99 },
        ],
      },
    ];

    await handler(await buildEvent(recs));
    expect(infos.length).toEqual(2);
    expect(infos[0][1]).toEqual({ records: 1, antebytes: 1, bytes: 0, segmentbytes: 0 });
    expect(infos[1][1]).toEqual({ records: 1, upserts: 1, failures: 0, logged: 0 });

    // add segment 3
    const recs2 = [
      { listenerEpisode, digest, type: "segmentbytes", segment: 3, timestamp: timestamp + 99 },
    ];
    await handler(await buildEvent(recs2));
    expect(infos.length).toEqual(5);
    expect(infos[2][1]).toEqual({ records: 1, antebytes: 0, bytes: 0, segmentbytes: 1 });
    expect(infos[4][1]).toEqual({ records: 1, upserts: 1, failures: 0, logged: 1 });

    // middle info is the postbyte impression
    expect(infos[3][0]).toEqual("impression");
    expect(infos[3][1]).toEqual({
      listenerEpisode,
      digest,
      requestUuid,
      type: "postbytes",
      timestamp: timestamp + 99,
      impressions: [{ segment: 3, flightId: 99, timestamp: timestamp + 99 }],
    });

    // add overall download + extras, and dup of segment 3
    const recs3 = [
      { listenerEpisode, digest, type: "bytes", timestamp: timestamp + 11, durations: [1, 2, 3] },
      { listenerEpisode, digest, type: "bytes", timestamp: timestamp + 22, durations: [1, 2, 3] },
      { listenerEpisode, digest, type: "segmentbytes", segment: 3, timestamp: timestamp + 111 },
    ];
    await handler(await buildEvent(recs3));
    expect(infos.length).toEqual(8);
    expect(infos[5][1]).toEqual({ records: 3, antebytes: 0, bytes: 2, segmentbytes: 1 });
    expect(infos[7][1]).toEqual({ records: 3, upserts: 1, failures: 0, logged: 1 });

    // middle info is the postbyte download
    expect(infos[6][0]).toEqual("impression");
    expect(infos[6][1]).toEqual({
      listenerEpisode,
      digest,
      requestUuid,
      type: "postbytes",
      timestamp: timestamp + 11,
      download: { the: "download", timestamp: timestamp + 11 },
      impressions: [],
      durations: [1, 2, 3],
      types: "",
    });
  });

  it("inserts segments then redirects", async () => {
    const listenerEpisode = v4();
    const digest = "the-digest";
    const timestamp = Date.now();
    const requestUuid = v4();
    const recs = [
      { listenerEpisode, digest, type: "bytes", timestamp: timestamp + 11, durations: [1, 2, 3] },
      { listenerEpisode, digest, type: "bytes", timestamp: timestamp + 22, durations: [1, 2, 3] },
      { listenerEpisode, digest, type: "segmentbytes", segment: 3, timestamp: timestamp + 111 },
    ];

    await handler(await buildEvent(recs));
    expect(infos.length).toEqual(2);
    expect(infos[0][1]).toEqual({ records: 3, antebytes: 0, bytes: 2, segmentbytes: 1 });
    expect(infos[1][1]).toEqual({ records: 3, upserts: 1, failures: 0, logged: 0 });

    // add redirect antebytes
    const recs2 = [
      {
        listenerEpisode,
        digest,
        timestamp,
        requestUuid,
        type: "antebytes",
        download: { the: "download" },
        impressions: [{ segment: 3, flightId: 99, isDuplicate: true }],
      },
    ];
    await handler(await buildEvent(recs2));
    expect(infos.length).toEqual(5);
    expect(infos[2][1]).toEqual({ records: 1, antebytes: 1, bytes: 0, segmentbytes: 0 });
    expect(infos[4][1]).toEqual({ records: 1, upserts: 1, failures: 0, logged: 1 });

    // middle info is the postbyte download + impression
    expect(infos[3][0]).toEqual("impression");
    expect(infos[3][1]).toEqual({
      listenerEpisode,
      digest,
      requestUuid,
      type: "postbytes",
      timestamp: timestamp + 11,
      download: { the: "download", timestamp: timestamp + 11 },
      impressions: [{ segment: 3, flightId: 99, isDuplicate: true, timestamp: timestamp + 111 }],
      durations: [1, 2, 3],
      types: "",
    });
  });
});
