import * as timestamp from "./timestamp";

describe("timestamp", () => {
  describe(".toEpochSeconds", () => {
    it("converts milliseconds timestamps, when after the year 2000", () => {
      expect(timestamp.toEpochSeconds(1490827132000)).toEqual(1490827132);
      expect(timestamp.toEpochSeconds(1490827132999)).toEqual(1490827132);
      expect(timestamp.toEpochSeconds(946684800001)).toEqual(946684800);
      expect(timestamp.toEpochSeconds(946684700000)).toEqual(946684700000);
    });
    it("leaves seconds alone", () => {
      expect(timestamp.toEpochSeconds(1490827132)).toEqual(1490827132);
      expect(timestamp.toEpochSeconds(0)).toEqual(0);
      expect(timestamp.toEpochSeconds(4102444800)).toEqual(4102444800);
    });
  });

  describe(".toEpochMilliseconds", () => {
    it("converts seconds timestamps, when before the year 2000", () => {
      expect(timestamp.toEpochMilliseconds(1490827132000)).toEqual(1490827132000);
      expect(timestamp.toEpochMilliseconds(1490827132999)).toEqual(1490827132999);
      expect(timestamp.toEpochMilliseconds(946684800001)).toEqual(946684800001);
      expect(timestamp.toEpochMilliseconds(946684700000)).toEqual(946684700000000);
    });
  });

  describe(".toDateString", () => {
    it("gets a date string for milliseconds timestamps", () => {
      expect(timestamp.toDateString(1490827132000)).toEqual("20170329");
      expect(timestamp.toDateString(1490827132999)).toEqual("20170329");
      expect(timestamp.toDateString(1490831999999)).toEqual("20170329");
      expect(timestamp.toDateString(1490832000000)).toEqual("20170330");
      expect(timestamp.toDateString(946684800001)).toEqual("20000101");
      expect(timestamp.toDateString(946684700000)).toEqual("+0319690330");
    });

    it("gets a date string for second timestamps", () => {
      expect(timestamp.toDateString(1490827132)).toEqual("20170329");
      expect(timestamp.toDateString(0)).toEqual("19700101");
      expect(timestamp.toDateString(4102444800)).toEqual("21000101");
    });
  });

  describe(".toISODateString", () => {
    it("gets an iso date string for milliseconds timestamps", () => {
      expect(timestamp.toISODateString(1490827132000)).toEqual("2017-03-29");
      expect(timestamp.toISODateString(1490827132999)).toEqual("2017-03-29");
      expect(timestamp.toISODateString(1490831999999)).toEqual("2017-03-29");
      expect(timestamp.toISODateString(1490832000000)).toEqual("2017-03-30");
      expect(timestamp.toISODateString(946684800001)).toEqual("2000-01-01");
      expect(timestamp.toISODateString(946684700000)).toEqual("+031969-03-30");
    });

    it("gets an iso date string for second timestamps", () => {
      expect(timestamp.toISODateString(1490827132)).toEqual("2017-03-29");
      expect(timestamp.toISODateString(0)).toEqual("1970-01-01");
      expect(timestamp.toISODateString(4102444800)).toEqual("2100-01-01");
    });
  });

  describe(".toISOExtendedZ", () => {
    it("gets a date string without the Z", () => {
      expect(timestamp.toISOExtendedZ(1490827132)).toEqual("2017-03-29T22:38:52Z");
      expect(timestamp.toISOExtendedZ(4102444800)).toEqual("2100-01-01T00:00:00Z");
      expect(timestamp.toISOExtendedZ(1490827132010)).toEqual("2017-03-29T22:38:52Z");
    });
  });

  describe(".toDigest", () => {
    it("creates digests based on the utc-day of a timestamp", () => {
      const d1 = timestamp.toDigest("any-str", 1490827132000);
      const d2 = timestamp.toDigest("any-str2", 1490827132000);
      const d3 = timestamp.toDigest("any-str", 1490827132999);
      const d4 = timestamp.toDigest("any-str", 1490831999999);
      const d5 = timestamp.toDigest("any-str", 1490832000000);
      expect(d1).not.toEqual(d2);
      expect(d1).toEqual(d3);
      expect(d1).toEqual(d4);
      expect(d1).not.toEqual(d5);
    });
  });
});
