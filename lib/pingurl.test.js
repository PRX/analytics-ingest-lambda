import { jest } from "@jest/globals";
import log from "lambda-log";
import nock from "nock";
import * as pingurl from "./pingurl";

describe("pingurl", () => {
  describe(".ping", () => {
    it("gets http urls", async () => {
      const scope = nock("http://www.foo.bar").get("/the/path").reply(200);
      expect(await pingurl.ping("http://www.foo.bar/the/path")).toEqual(true);
      expect(scope.isDone()).toEqual(true);
    });

    it("gets https urls", async () => {
      const scope = nock("https://www.foo.bar").get("/the/path").reply(200);
      expect(await pingurl.ping("https://www.foo.bar/the/path")).toEqual(true);
      expect(scope.isDone()).toEqual(true);
    });

    it("handles bad urls", async () => {
      await expect(pingurl.ping("not a url")).rejects.toThrow(/invalid ping url/i);
    });

    it("handles hostname errors", async () => {
      await expect(pingurl.ping("http://this.is.not.a.real.domain")).rejects.toThrow(/ENOTFOUND/i);
    });

    it("is totally ok with any 2XX status code", async () => {
      const scope = nock("http://www.foo.bar").get("/the/path").reply(204);
      expect(await pingurl.ping("http://www.foo.bar/the/path")).toEqual(true);
      expect(scope.isDone()).toEqual(true);
    });

    it("throws http errors", async () => {
      const scope = nock("http://www.foo.bar").get("/").reply(404);
      await expect(pingurl.ping("http://www.foo.bar/")).rejects.toThrow(/http 404 from/i);
      expect(scope.isDone()).toEqual(true);
    });

    it("retries 502 errors", async () => {
      jest.spyOn(log, "warn").mockReturnValue();
      const scope = nock("http://foo").get("/").times(3).reply(502);
      await expect(pingurl.ping("http://foo/", null, 1000, 1)).rejects.toThrow(/http 502 from/i);
      expect(scope.isDone()).toEqual(true);
    });

    it("times out with a nocked delay", async () => {
      nock("http://foo").get("/to").delay(100).reply(200);
      await expect(pingurl.ping("http://foo/to", null, 10)).rejects.toThrow(/http timeout from/i);
    });

    it("times out with a nocked redirect-delay", async () => {
      nock("http://foo").get("/re").reply(302, null, { Location: "http://bar/to" });
      nock("http://bar").get("/to").delay(100).reply(200);
      await expect(pingurl.ping("http://foo/re", null, 10)).rejects.toThrow(/http timeout from/i);
    });

    it("proxies headers to request", async () => {
      const opts = {
        reqheaders: {
          "User-Agent": "foo",
          Referer: "bar",
          "X-Forwarded-For": "9.8.7.0",
        },
      };
      const scope = nock("http://www.foo.bar", opts).get("/the/path").reply(200);
      const input = { remoteAgent: "foo", remoteIp: "9.8.7.6", remoteReferrer: "bar" };

      expect(await pingurl.ping("http://www.foo.bar/the/path", input)).toEqual(true);
      expect(scope.isDone()).toEqual(true);
    });

    it("follows redirects", async () => {
      const hdrs = { Location: "http://www.foo.bar/redirected" };
      const scope1 = nock("http://www.foo.bar").get("/redirect").reply(302, undefined, hdrs);
      const scope2 = nock("http://www.foo.bar").get("/redirected").reply(200);

      expect(await pingurl.ping("http://www.foo.bar/redirect")).toEqual(true);
      expect(scope1.isDone()).toEqual(true);
      expect(scope2.isDone()).toEqual(true);
    });
  });

  describe(".post", () => {
    it("posts json data", async () => {
      const data = { some: "data" };
      const json = JSON.stringify(data);
      const opts = {
        reqheaders: {
          "content-length": json.length,
          "content-type": "application/json",
          "user-agent": "PRX Dovetail Analytics Ingest",
        },
      };
      const scope = nock("https://www.foo.bar", opts).post("/the/path", json).reply(202);
      expect(await pingurl.post("https://www.foo.bar/the/path", data)).toEqual(true);
      expect(scope.isDone()).toEqual(true);
    });

    it("posts json data with an authorization header", async () => {
      const data = { some: "data" };
      const json = JSON.stringify(data);
      const opts = {
        reqheaders: {
          authorization: "PRXToken abcd1234",
          "content-length": json.length,
          "content-type": "application/json",
          "user-agent": "PRX Dovetail Analytics Ingest",
        },
      };
      const scope = nock("https://www.foo.bar", opts).post("/the/path", json).reply(202);
      expect(await pingurl.post("https://www.foo.bar/the/path", data, "abcd1234")).toEqual(true);
      expect(scope.isDone()).toEqual(true);
    });
  });

  describe(".parseHeaders", () => {
    it("parses headers from input data", () => {
      expect(pingurl.parseHeaders()).toEqual({});
      expect(pingurl.parseHeaders({ remoteAgent: "" })).toEqual({});
      expect(pingurl.parseHeaders({ remoteAgent: "foo" })).toEqual({ "User-Agent": "foo" });
      expect(pingurl.parseHeaders({ remoteReferrer: "http://www.prx.org" })).toEqual({
        Referer: "http://www.prx.org",
      });
    });

    it("masks ips in x-forwarded-for", () => {
      const parseXff = (remoteIp) => {
        return pingurl.parseHeaders({ remoteIp })["X-Forwarded-For"];
      };

      expect(parseXff("")).toEqual(undefined);
      expect(parseXff("66.6.44.4")).toEqual("66.6.44.0");
      expect(parseXff("2804:18:1012:6b65:1:3:3561:14b8")).toEqual("2804:18:1012:6b65::");
      expect(parseXff(",blah ,  66.6.44.4")).toEqual("66.6.44.0");
      expect(parseXff("192.168.0.1,66.6.44.4")).toEqual("192.168.0.0, 66.6.44.4");
    });

    it("provides full ips in x-forwarded-for", () => {
      const parseXff = (remoteIp) => {
        return pingurl.parseHeaders({ remoteIp, fullIp: true })["X-Forwarded-For"];
      };

      expect(parseXff("")).toEqual(undefined);
      expect(parseXff("66.6.44.4")).toEqual("66.6.44.4");
      expect(parseXff("1:2:3:4:5:6:7:8")).toEqual("1:2:3:4:5:6:7:8");
      expect(parseXff(",blah ,  66.6.44.4")).toEqual("66.6.44.4");
      expect(parseXff("192.168.0.1,66.6.44.4")).toEqual("192.168.0.1, 66.6.44.4");
    });
  });
});
