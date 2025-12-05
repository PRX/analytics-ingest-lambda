import { jest } from "@jest/globals";
import log from "lambda-log";
import nock from "nock";
import * as index from "./index-pingbacks";
import { buildEvent } from "./test/support";
import testRecs from "./test/support/test-records";

describe("index-pingbacks", () => {
  describe(".handler", () => {
    it("pings and increments", async () => {
      process.env.DOVETAIL_ROUTER_HOSTS = "host1.dt.test,host2.dt.test";
      process.env.DOVETAIL_ROUTER_API_TOKENS = "tok1,tok2";

      jest.spyOn(log, "info").mockReturnValue();
      jest.spyOn(log, "warn").mockReturnValue();

      const ping1 = nock("http://www.foo.bar").get("/ping1").reply(200);
      const ping2 = nock("http://www.foo.bar").get("/ping2").reply(404);
      const ping3 = nock("http://www.foo.bar").get("/ping3").reply(200);
      const ping4 = nock("http://www.adzerk.bar").get("/ping4").reply(200);
      const ping5 = nock("http://www.adzerk.bar").get("/ping5").reply(404);

      const path = "/api/v1/flight_increments/2017-02-21";
      const incr1 = nock("https://host1.dt.test").post(path, '{"78":2,"107":1}').reply(202);
      const incr2 = nock("https://host2.dt.test").post(path, '{"78":2,"107":1}').reply(404);

      await index.handler(await buildEvent(testRecs));
      expect(log.info.mock.calls.length).toEqual(4);
      expect(log.info.mock.calls[0][0]).toEqual("Starting Pingbacks");
      expect(log.info.mock.calls[0][1]).toEqual({ records: 7, pingbacks: 4, increments: 3 });
      expect(log.info.mock.calls[1][0]).toEqual("PINGED");
      expect(log.info.mock.calls[1][1]).toEqual({ url: "http://www.foo.bar/ping1" });
      expect(log.info.mock.calls[2][0]).toEqual("PINGED");
      expect(log.info.mock.calls[2][1]).toEqual({ url: "http://www.adzerk.bar/ping4" });
      expect(log.info.mock.calls[3][0]).toEqual("Finished Pingbacks");
      expect(log.info.mock.calls[3][1]).toEqual({
        records: 7,
        pingbacks: 2,
        pingfails: 2,
        increments: 3,
      });

      expect(log.warn.mock.calls.length).toEqual(3);
      expect(log.warn.mock.calls[0][0]).toMatch(/PINGFAIL error: http 404/i);
      expect(log.warn.mock.calls[0][1]).toEqual({ url: "http://www.foo.bar/ping2" });
      expect(log.warn.mock.calls[1][0]).toMatch(/PINGFAIL error: http 404/i);
      expect(log.warn.mock.calls[1][1]).toEqual({ url: "http://www.adzerk.bar/ping5" });
      expect(log.warn.mock.calls[2][0]).toMatch(/INCRFAIL error: http 404/i);
      expect(log.warn.mock.calls[2][1].url).toMatch("host2.dt.test");

      expect(ping1.isDone()).toEqual(true);
      expect(ping2.isDone()).toEqual(true);
      expect(ping3.isDone()).toEqual(false);
      expect(ping4.isDone()).toEqual(true);
      expect(ping5.isDone()).toEqual(true);
      expect(incr1.isDone()).toEqual(true);
      expect(incr2.isDone()).toEqual(true);
    });

    it("ignores unknown types and duplicates", async () => {
      process.env.DOVETAIL_ROUTER_HOSTS = "host1.dt.test,host2.dt.test";
      process.env.DOVETAIL_ROUTER_API_TOKENS = "tok1,tok2";

      jest.spyOn(log, "info").mockReturnValue();

      const pings = ["http://some.where"];
      const event = await buildEvent([
        { type: "postbytes", timestamp: 1 },
        { type: "postbytes", timestamp: 1, download: {}, impressions: [{ isDuplicate: true }] },
        { type: "postbytes", timestamp: 1, impressions: [{ isDuplicate: true, pings }] },
        { type: "bytes", timestamp: 1, download: {}, impressions: [{}] },
        { type: "whatev", timestamp: 1, download: {}, impressions: [{}] },
      ]);

      await index.handler(event);
      expect(log.info.mock.calls.length).toEqual(2);
      expect(log.info.mock.calls[0][0]).toEqual("Starting Pingbacks");
      expect(log.info.mock.calls[0][1]).toEqual({ records: 5, pingbacks: 0, increments: 0 });
      expect(log.info.mock.calls[1][0]).toEqual("Finished Pingbacks");
      expect(log.info.mock.calls[1][1]).toEqual({
        records: 5,
        pingbacks: 0,
        increments: 0,
        pingfails: 0,
      });
    });

    it("checks full IP permissions", async () => {
      jest.spyOn(log, "info").mockReturnValue();

      process.env.DOVETAIL_ROUTER_HOSTS = "host1.dt.test";
      nock("https://host1.dt.test")
        .post(/.+/, () => true)
        .reply(202);

      // mock requests, recording xff and query params
      const pings = [];
      const ips = [];
      const ipmasks = [];
      const xffs = [];
      ["one", "two", "three", "four"].forEach((num, idx) => {
        pings.push(`http://ping.${num}{?ip,ipmask}`);
        nock(`http://ping.${num}`)
          .get("/")
          .query((q) => {
            ips[idx] = q.ip;
            ipmasks[idx] = q.ipmask;
            return true;
          })
          .reply(200, function () {
            xffs[idx] = this.req.headers["x-forwarded-for"];
          });
      });

      const impressions = [
        { pings: [pings[0], pings[1], pings[2]], pingFullIps: [false, true, null] },
        { pings: [pings[3]] },
      ];
      const remoteIp = "12.34.56.78";
      const event = await buildEvent([
        { type: "postbytes", timestamp: 1, download: {}, remoteIp, impressions },
      ]);
      await index.handler(event);

      // one should get downgraded
      expect(ips[0]).toEqual("12.34.56.0");
      expect(ipmasks[0]).toEqual("12.34.56.0");
      expect(xffs[0]).toEqual("12.34.56.0");

      // two gets full ips for all but the mask
      expect(ips[1]).toEqual("12.34.56.78");
      expect(ipmasks[1]).toEqual("12.34.56.0");
      expect(xffs[1]).toEqual("12.34.56.78");

      // three is also downgraded
      expect(ips[2]).toEqual("12.34.56.0");
      expect(ipmasks[2]).toEqual("12.34.56.0");
      expect(xffs[2]).toEqual("12.34.56.0");

      // four has no "pingFullIps" array - revert to old mask-XFF functionality
      expect(ips[3]).toEqual("12.34.56.78");
      expect(ipmasks[3]).toEqual("12.34.56.0");
      expect(xffs[3]).toEqual("12.34.56.0");
    });
  });

  describe(".formatIncrements", () => {
    it("groups increments by date and flight id", () => {
      const res = index.formatIncrements([
        { timestamp: 1, flightId: 111 },
        { timestamp: 86400, flightId: 111 },
        { timestamp: 2, flightId: 111 },
        { timestamp: 3, flightId: 222 },
        { timestamp: 86401, flightId: 222 },
        { timestamp: 86402, flightId: 333 },
      ]);

      expect(Object.keys(res)).toEqual(["1970-01-01", "1970-01-02"]);
      expect(res["1970-01-01"]).toEqual({ 111: 2, 222: 1 });
      expect(res["1970-01-02"]).toEqual({ 111: 1, 222: 1, 333: 1 });
    });
  });
});
