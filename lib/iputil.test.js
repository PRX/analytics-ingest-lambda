import * as iputil from "./iputil";

describe("iputil", () => {
  describe(".clean", () => {
    it("cleans ips", () => {
      expect(iputil.clean("")).toEqual(undefined);
      expect(iputil.clean(",blah")).toEqual(undefined);
      expect(iputil.clean(",99.99.99.99")).toEqual("99.99.99.99");
      expect(iputil.clean(", , 66.6.44.4 ,99.99.99.99")).toEqual("66.6.44.4");
    });
  });

  describe(".cleanAll", () => {
    it("cleans a list of ips", () => {
      expect(iputil.cleanAll("")).toEqual(undefined);
      expect(iputil.cleanAll(",blah")).toEqual(undefined);
      expect(iputil.cleanAll(", , 66.6.44.4 ,99.99.99.99, blah")).toEqual("66.6.44.4, 99.99.99.99");
    });
  });

  describe(".mask", () => {
    it("masks ips", () => {
      expect(iputil.mask("blah")).toEqual("blah");
      expect(iputil.mask("1234.5678.1234.5678")).toEqual("1234.5678.1234.5678");
      expect(iputil.mask("192.168.0.1")).toEqual("192.168.0.0");
      expect(iputil.mask("2804:18:1012:6b65:1:3:3561:14b8")).toEqual("2804:18:1012:6b65::");
    });
  });

  describe(".maskLeft", () => {
    it("masks the leftmost x-forwarded-for ip", () => {
      expect(iputil.maskLeft("66.6.44.4, 99.99.99.99")).toEqual("66.6.44.0, 99.99.99.99");
      expect(iputil.maskLeft("unknown, 99.99.99.99, 127.0.0.1")).toEqual(
        "unknown, 99.99.99.99, 127.0.0.1",
      );
      expect(iputil.maskLeft("1:2:3:4:5::, 127.0.0.1")).toEqual("1:2:3:4::, 127.0.0.1");
    });
  });

  describe(".ipV4Only", () => {
    it("only ipv4s", () => {
      expect(iputil.ipV4Only("blah")).toEqual(undefined);
      expect(iputil.ipV4Only("1234.5678.1234.5678")).toEqual(undefined);
      expect(iputil.ipV4Only("192.168.0.1")).toEqual("192.168.0.1");
      expect(iputil.ipV4Only("2804:18:1012:6b65:1:3:3561:14b8")).toEqual(undefined);
    });
  });

  describe(".fixed", () => {
    it("converts to fixed length strings", () => {
      expect(iputil.fixed("blah")).toEqual("blah");
      expect(iputil.fixed("1234.5678.1234.5678")).toEqual("1234.5678.1234.5678");
      expect(iputil.fixed("192.68.0.1")).toEqual("192.068.000.001");
      expect(iputil.fixed("2804:18:1012::61:14b8")).toEqual(
        "2804:0018:1012:0000:0000:0000:0061:14b8",
      );
    });
  });

  describe(".fixedKind", () => {
    it("converts to fixed length strings and returns the kind of ip", () => {
      expect(iputil.fixedKind("blah")).toEqual(["blah", null]);
      expect(iputil.fixedKind("1234.5678.1234.5678")).toEqual(["1234.5678.1234.5678", null]);
      expect(iputil.fixedKind("192.68.0.1")).toEqual(["192.068.000.001", "v4"]);
      expect(iputil.fixedKind("2804:18:1012::61:14b8")).toEqual([
        "2804:0018:1012:0000:0000:0000:0061:14b8",
        "v6",
      ]);
    });
  });
});
