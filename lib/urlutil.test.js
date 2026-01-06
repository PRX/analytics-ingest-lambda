import * as uuid from "uuid";
import * as urlutil from "./urlutil";

const testImp = (params = {}) => {
  return {
    url: "/99/the/path.mp3?foo=bar",
    feederPodcast: 1234,
    feederEpisode: "episode-guid",
    remoteAgent: "agent-string",
    remoteIp: "127.0.0.1, 127.0.0.2, 127.0.0.3",
    remoteReferrer: "http://www.prx.org/",
    timestamp: 1507234920,
    listenerId: "listener-id",
    listenerEpisode: "listener-episode",
    adId: 9,
    campaignId: 8,
    creativeId: 7,
    flightId: 6,
    durations: [24.03, 32.31, 840.72, 19.96, 495.51, 39.761, 6.2],
    types: "aaoaohi",
    segment: 3,
    ...params,
  };
};

describe("urlutil", () => {
  describe(".expand", () => {
    it("expands non-transformed params", () => {
      const nonTransforms = [
        "ad",
        "agent",
        "campaign",
        "creative",
        "episode",
        "flight",
        "listener",
        "listenerepisode",
        "podcast",
        "referer",
      ];
      const url = urlutil.expand(`http://foo/{?${nonTransforms.join(",")}}`, testImp());
      const params = Object.fromEntries(URL.parse(url).searchParams);
      expect(url).toMatch(/^http:\/\/foo\/\?/);
      expect(params.ad).toEqual("9");
      expect(params.agent).toEqual("agent-string");
      expect(params.campaign).toEqual("8");
      expect(params.creative).toEqual("7");
      expect(params.episode).toEqual("episode-guid");
      expect(params.flight).toEqual("6");
      expect(params.listener).toEqual("listener-id");
      expect(params.listenerepisode).toEqual("listener-episode");
      expect(params.podcast).toEqual("1234");
      expect(params.referer).toEqual("http://www.prx.org/");
    });

    it("expands non templates", () => {
      const url = "https://some/host/i.png?foo=bar&anything=else";
      expect(urlutil.expand(url)).toEqual(url);
    });

    it("throws invalid template errors", () => {
      const throws = () => {
        const url = "htps///some/host/<showid>/<adid>/<pos>?IP={ip}";
        urlutil.expand(url, testImp());
      };
      expect(throws).toThrow(/invalid url template/i);
    });

    it("gets the md5 digest for the agent", () => {
      const url = urlutil.expand("http://foo/?ua={agentmd5}", testImp());
      expect(url).toEqual("http://foo/?ua=da08af6021d3ec8b8d27558ca92c314e");
    });

    it("cleans ip addresses", () => {
      const url1 = urlutil.expand("http://foo/{?ip}", testImp());
      const url2 = urlutil.expand("http://foo/{?ip}", testImp({ remoteIp: "what , 127.0.0.1" }));
      const url3 = urlutil.expand("http://foo/{?ip}", testImp({ remoteIp: "  " }));
      expect(url1).toEqual("http://foo/?ip=127.0.0.1");
      expect(url2).toEqual("http://foo/?ip=127.0.0.1");
      expect(url3).toEqual("http://foo/");
    });

    it("masks ip addresses", () => {
      const url1 = urlutil.expand("http://foo/{?ipmask}", testImp());
      const url2 = urlutil.expand("http://foo/{?ipmask}", testImp({ remoteIp: "wha,127.0.0.1" }));
      const url3 = urlutil.expand("http://foo/{?ipmask}", testImp({ remoteIp: "   " }));
      expect(url1).toEqual("http://foo/?ipmask=127.0.0.0");
      expect(url2).toEqual("http://foo/?ipmask=127.0.0.0");
      expect(url3).toEqual("http://foo/");
    });

    it("does ipv4 addresses", () => {
      const url1 = urlutil.expand("http://foo/{?ipv4}", testImp());
      const url2 = urlutil.expand("http://foo/{?ipv4}", testImp({ remoteIp: "what , 127.0.0.1" }));
      const url3 = urlutil.expand("http://foo/{?ipv4}", testImp({ remoteIp: "  " }));
      const url4 = urlutil.expand("http://foo/{?ipv4}", testImp({ remoteIp: "a, 1:2:3:4::,ev" }));
      expect(url1).toEqual("http://foo/?ipv4=127.0.0.1");
      expect(url2).toEqual("http://foo/?ipv4=127.0.0.1");
      expect(url3).toEqual("http://foo/");
      expect(url4).toEqual("http://foo/");
    });

    it("downgrades full IP addresses", () => {
      const remoteIp = "127.0.0.1";
      const url1 = urlutil.expand("http://foo/{?ip}", testImp({ remoteIp, fullIp: true }));
      const url2 = urlutil.expand("http://foo/{?ip}", testImp({ remoteIp, fullIp: false }));
      const url3 = urlutil.expand("http://foo/{?ip}", testImp({ remoteIp, fullIp: null }));
      const url4 = urlutil.expand("http://foo/{?ipv4}", testImp({ remoteIp, fullIp: true }));
      const url5 = urlutil.expand("http://foo/{?ipv4}", testImp({ remoteIp, fullIp: false }));
      const url6 = urlutil.expand("http://foo/{?ipv4}", testImp({ remoteIp, fullIp: null }));
      expect(url1).toEqual("http://foo/?ip=127.0.0.1");
      expect(url2).toEqual("http://foo/?ip=127.0.0.0");
      expect(url3).toEqual("http://foo/?ip=127.0.0.0");
      expect(url4).toEqual("http://foo/?ipv4=127.0.0.1");
      expect(url5).toEqual("http://foo/?ipv4=127.0.0.0");
      expect(url6).toEqual("http://foo/?ipv4=127.0.0.0");
    });

    it("returns timestamps in milliseconds", () => {
      const url1 = urlutil.expand("http://foo/{?timestamp}", testImp());
      const url2 = urlutil.expand("http://foo/{?timestamp}", testImp({ timestamp: 1507234920010 }));
      expect(url1).toEqual("http://foo/?timestamp=1507234920000");
      expect(url2).toEqual("http://foo/?timestamp=1507234920010");
    });

    it("does not collide on 32 bit random ints very often", () => {
      const many = Array(1000)
        .fill()
        .map(() => {
          return urlutil.expand("http://foo/{randomint}", testImp({ requestUuid: uuid.v4() }));
        });
      expect(new Set(many).size).toEqual(1000);
      many.forEach((url) => {
        const num = parseInt(url.split("/").pop(), 10);
        const bitCount = num.toString(2).match(/1/g).length;
        expect(num).toBeGreaterThan(0);
        expect(num).toBeLessThanOrEqual(2147483647);
        expect(bitCount).toBeLessThanOrEqual(32);
      });
    });

    it("returns random strings based on timestamp + listenerepisode + ad", () => {
      const url1 = urlutil.expand("http://foo/{?randomstr}", testImp());
      const url2 = urlutil.expand("http://foo/{?randomstr}", testImp());
      const url3 = urlutil.expand("http://foo/{?randomstr}", testImp({ timestamp: 9999999 }));
      const url4 = urlutil.expand("http://foo/{?randomstr}", testImp({ listenerEpisode: "ch" }));
      const url5 = urlutil.expand("http://foo/{?randomstr}", testImp({ adId: 8 }));
      expect(url1).toEqual(url2);
      expect(url1).not.toEqual(url3);
      expect(url1).not.toEqual(url4);
      expect(url1).not.toEqual(url5);
      expect(url3).not.toEqual(url4);
      expect(url4).not.toEqual(url5);
    });

    it("reassembles original request url", () => {
      const url = urlutil.expand("http://foo/?ru={url}", testImp());
      expect(url).toEqual("http://foo/?ru=dovetail.prxu.org%2F99%2Fthe%2Fpath.mp3%3Ffoo%3Dbar");
    });

    it("returns ad position data", () => {
      const tpl = "http://foo{?totalduration,adpodposition,adpodoffsetstart,adposition}";
      const url = urlutil.expand(tpl, testImp());
      expect(url).toContain("totalduration=1458.491");
      expect(url).toContain("adpodposition=2");
      expect(url).toContain("adpodoffsetstart=897.06");
      expect(url).toContain("adposition=a");
    });
  });

  describe(".count", () => {
    it("counts by hostname", () => {
      expect(urlutil.count({}, null)).toEqual({});
      expect(urlutil.count({ start: 99 }, undefined)).toEqual({ start: 99 });
      expect(urlutil.count({}, "http://foo.gov/bar")).toEqual({ "foo.gov": 1 });
      expect(urlutil.count({ "foo.gov": 10 }, "https://foo.gov/bar")).toEqual({ "foo.gov": 11 });
    });
  });
});
