import { jest } from "@jest/globals";
import log from "lambda-log";
import nock from "nock";
import * as index from "./index-pingbacks";
import { buildEvent } from "./test/support";
import testRecs from "./test/support/test-records";

describe("index-pingbacks", () => {
  describe(".handler", () => {
    it("pings pingbacks", async () => {
      jest.spyOn(log, "info").mockReturnValue();
      jest.spyOn(log, "warn").mockReturnValue();

      const ping1 = nock("http://www.foo.bar").get("/ping1").reply(200);
      const ping2 = nock("http://www.foo.bar").get("/ping2").reply(404);
      const ping3 = nock("http://www.foo.bar").get("/ping3").reply(200);
      const ping4 = nock("http://www.adzerk.bar").get("/ping4").reply(200);
      const ping5 = nock("http://www.adzerk.bar").get("/ping5").reply(404);

      await index.handler(await buildEvent(testRecs));

      expect(log.info.mock.calls.length).toEqual(4);
      expect(log.info.mock.calls[0][0]).toEqual("Starting Pingbacks");
      expect(log.info.mock.calls[0][1]).toEqual({ records: 7, pingbacks: 4, increments: 3 });
      expect(log.info.mock.calls[1][0]).toEqual("PINGED");
      expect(log.info.mock.calls[1][1]).toEqual({ url: "http://www.foo.bar/ping1" });
      expect(log.info.mock.calls[2][0]).toEqual("PINGED");
      expect(log.info.mock.calls[2][1]).toEqual({ url: "http://www.adzerk.bar/ping4" });
      expect(log.info.mock.calls[3][0]).toEqual("Finished Pingbacks");
      expect(log.info.mock.calls[3][1]).toEqual({ records: 7, pingbacks: 2, pingfails: 2 });

      expect(log.warn.mock.calls.length).toEqual(2);
      expect(log.warn.mock.calls[0][0]).toMatch(/PINGFAIL error: http 404/i);
      expect(log.warn.mock.calls[0][1]).toEqual({ url: "http://www.foo.bar/ping2" });
      expect(log.warn.mock.calls[1][0]).toMatch(/PINGFAIL error: http 404/i);
      expect(log.warn.mock.calls[1][1]).toEqual({ url: "http://www.adzerk.bar/ping5" });

      expect(ping1.isDone()).toEqual(true);
      expect(ping2.isDone()).toEqual(true);
      expect(ping3.isDone()).toEqual(false);
      expect(ping4.isDone()).toEqual(true);
      expect(ping5.isDone()).toEqual(true);
    });
  });
});
