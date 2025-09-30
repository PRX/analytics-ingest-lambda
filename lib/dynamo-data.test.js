import { deflate } from "./dynamo";
import DynamoData from "./dynamo-data";

describe("DynamoData", () => {
  describe(".encodeSegment", () => {
    it("encodes segment strings", () => {
      expect(DynamoData.encodeSegment({ timestamp: 1490827132 })).toEqual("1490827132000");
      expect(DynamoData.encodeSegment({ timestamp: 1490827132000 })).toEqual("1490827132000");

      const timestamp = 1490827132123;
      const type = "segmentbytes";
      expect(DynamoData.encodeSegment({ timestamp, type, segment: 0 })).toEqual("1490827132123.0");
      expect(DynamoData.encodeSegment({ timestamp, type, segment: 2 })).toEqual("1490827132123.2");
      expect(DynamoData.encodeSegment({ timestamp, type: "o", segment: 2 })).toEqual(
        "1490827132123",
      );
    });
  });

  describe(".decodeSegment", () => {
    it("decodes segment strings", () => {
      expect(DynamoData.decodeSegment("1490827132")).toEqual([1490827132000, null]);
      expect(DynamoData.decodeSegment("1490827132000")).toEqual([1490827132000, null]);
      expect(DynamoData.decodeSegment("1490827132.0")).toEqual([1490827132000, 0]);
      expect(DynamoData.decodeSegment("1490827132.4")).toEqual([1490827132000, 4]);
      expect(DynamoData.decodeSegment("1490827132.aaaa")).toEqual([1490827132000, "aaaa"]);
    });
  });

  describe("#payload", () => {
    it("returns the set payload", async () => {
      const payload = { foo: "bar1" };
      const result = { Attributes: { payload: { B: await deflate({ foo: "bar2" }) } } };
      const data = new DynamoData({ payload, result });
      expect(await data.payload()).toEqual({ foo: "bar1" });
    });

    it("returns the old payload", async () => {
      const result = { Attributes: { payload: { B: await deflate({ foo: "bar2" }) } } };
      const data = new DynamoData({ result });
      expect(await data.payload()).toEqual({ foo: "bar2" });
    });

    it("merges set extras", async () => {
      const payload = { foo: "bar1" };
      const extras = { extra: "stuff1" };
      const result = { Attributes: { extras: { S: JSON.stringify({ extra: "stuff2" }) } } };
      const data = new DynamoData({ payload, extras, result });
      expect(await data.payload()).toEqual({ foo: "bar1", extra: "stuff1" });
    });

    it("merges old extras", async () => {
      const payload = { foo: "bar1" };
      const result = { Attributes: { extras: { S: JSON.stringify({ extra: "stuff2" }) } } };
      const data = new DynamoData({ payload, result });
      expect(await data.payload()).toEqual({ foo: "bar1", extra: "stuff2" });
    });

    it("returns null when no payload", async () => {
      const result = { Attributes: { extras: { S: JSON.stringify({ extra: "stuff2" }) } } };
      const data = new DynamoData({ result });
      expect(await data.payload()).toEqual(null);
    });
  });

  describe("#newSegments", () => {
    it("returns all segments the first time we set the payload", async () => {
      const payload = { foo: "bar1" };

      // 3 segments were previously in DDB
      const result = { Attributes: { segments: { SS: ["1111111", "1111111.2", "2222222.0"] } } };

      // just set 3 more (but 2 are dups)
      const segments = [1111111, "2222222.0", "2222222.1"];

      const data = new DynamoData({ payload, segments, result });
      expect(data.newSegments()).toEqual(["1111111", "1111111.2", "2222222.0", "2222222.1"]);
    });

    it("returns new segments if the payload was already set", async () => {
      const payload = { foo: "bar1" };

      // 3 segments were previously in DDB, plus the previous payload
      const result = {
        Attributes: {
          payload: { B: await deflate({ foo: "bar2" }) },
          segments: { SS: ["1111111", "1111111.2", "2222222.0"] },
        },
      };

      // just set 3 more (but 2 are dups)
      const segments = [1111111, "2222222.0", "2222222.1"];

      const data = new DynamoData({ payload, segments, result });
      expect(data.newSegments()).toEqual(["2222222.1"]);
    });

    it("dedups segments occurring on the same UTC day", async () => {
      const payload = { foo: "bar1" };

      const t1 = Date.parse("2024-01-01T12:00:00Z");
      const t2 = Date.parse("2024-01-01T13:00:00Z");
      const t3 = Date.parse("2024-01-02T01:00:00Z");

      // overall and some segment downloads at different timestamps
      const segments = [`${t1}.2`, `${t2}.2`, t1, t2, t3];
      const result = { Attributes: { segments: { SS: [`${t2}.0`, `${t3}.0`] } } };

      // earliest timestamp on the UTC day comes out
      const data = new DynamoData({ payload, segments, result });
      expect(data.newSegments()).toEqual([`${t1}`, `${t1}.2`, `${t2}.0`, `${t3}`, `${t3}.0`]);
    });

    it("returns nothing with no payload", async () => {
      const result = { Attributes: { segments: { SS: ["1111111"] } } };
      const segments = ["2222222.0"];
      const data = new DynamoData({ segments, result });
      expect(data.newSegments()).toEqual([]);
    });
  });

  describe("#postBytes", () => {
    const id = "listEp.dig";
    const payload = {
      msg: "something",
      type: "antebytes",
      timestamp: 1234,
      the: "record",
      download: {
        the: "download",
      },
      impressions: [
        { segment: 0, the: "seg0" },
        { segment: 2, the: "seg2" },
        { segment: 4, the: "seg4" },
      ],
    };

    it("formats postbyte records", async () => {
      const segments = ["1001", "1002.2"];
      const data = new DynamoData({ id, payload, segments });
      expect(await data.postBytes()).toEqual([
        {
          type: "antebytes",
          timestamp: 1001000,
          the: "record",
          listenerEpisode: "listEp",
          digest: "dig",
          download: { the: "download", timestamp: 1001000 },
          impressions: [{ segment: 2, the: "seg2", timestamp: 1002000 }],
        },
      ]);
    });

    it("returns 1 record per day", async () => {
      const segments = ["2002", "1001.0"];
      const data = new DynamoData({ id, payload, segments });
      expect(await data.postBytes()).toEqual([
        {
          type: "antebytes",
          timestamp: 1001000,
          the: "record",
          listenerEpisode: "listEp",
          digest: "dig",
          impressions: [{ segment: 0, the: "seg0", timestamp: 1001000 }],
        },
        {
          type: "antebytes",
          timestamp: 2002000,
          the: "record",
          listenerEpisode: "listEp",
          digest: "dig",
          download: { the: "download", timestamp: 2002000 },
          impressions: [],
        },
      ]);
    });

    it("returns empty if no new segments were found", async () => {
      const segments = ["9999999.9"];
      const data = new DynamoData({ id, payload, segments });
      expect(await data.postBytes()).toEqual([]);
    });
  });
});
