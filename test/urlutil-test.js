const _support = require("./support");
const urlutil = require("../lib/urlutil");
const URI = require("urijs");
const uuid = require("uuid");

describe("urlutil", () => {
  const TEST_IMPRESSION = (key, val) => {
    const data = {
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
    };
    if (key) {
      data[key] = val;
    }
    return data;
  };

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
    const url = urlutil.expand(`http://foo.bar/{?${nonTransforms.join(",")}}`, TEST_IMPRESSION());
    const params = URI(url).query(true);
    expect(url).to.match(/^http:\/\/foo\.bar\/\?/);
    expect(params.ad).to.equal("9");
    expect(params.agent).to.equal("agent-string");
    expect(params.campaign).to.equal("8");
    expect(params.creative).to.equal("7");
    expect(params.episode).to.equal("episode-guid");
    expect(params.flight).to.equal("6");
    expect(params.listener).to.equal("listener-id");
    expect(params.listenerepisode).to.equal("listener-episode");
    expect(params.podcast).to.equal("1234");
    expect(params.referer).to.equal("http://www.prx.org/");
  });

  it("expands non templates", () => {
    const url = "https://some/host/i.png?foo=bar&anything=else";
    expect(urlutil.expand(url)).to.equal(url);
  });

  it("throws invalid template errors", () => {
    let err = null;
    try {
      const url = "https://some/host/<showid>/<adid>/<pos>?IP={ip}";
      expect(urlutil.expand(url, TEST_IMPRESSION())).to.equal(url);
    } catch (e) {
      err = e;
    }
    if (err) {
      expect(err.message).to.match(/Invalid Literal "https:/);
    } else {
      expect.fail("should have thrown an error");
    }
  });

  it("gets the md5 digest for the agent", () => {
    const url = urlutil.expand("http://foo.bar/?ua={agentmd5}", TEST_IMPRESSION());
    expect(url).to.equal("http://foo.bar/?ua=da08af6021d3ec8b8d27558ca92c314e");
  });

  it("cleans ip addresses", () => {
    const url1 = urlutil.expand("http://foo.bar/{?ip}", TEST_IMPRESSION());
    const url2 = urlutil.expand(
      "http://foo.bar/{?ip}",
      TEST_IMPRESSION("remoteIp", "  what , 127.0.0.1"),
    );
    const url3 = urlutil.expand("http://foo.bar/{?ip}", TEST_IMPRESSION("remoteIp", "  "));
    expect(url1).to.equal("http://foo.bar/?ip=127.0.0.1");
    expect(url2).to.equal("http://foo.bar/?ip=127.0.0.1");
    expect(url3).to.equal("http://foo.bar/");
  });

  it("masks ip addresses", () => {
    const url1 = urlutil.expand("http://foo.bar/{?ipmask}", TEST_IMPRESSION());
    const url2 = urlutil.expand(
      "http://foo.bar/{?ipmask}",
      TEST_IMPRESSION("remoteIp", "  what , 127.0.0.1"),
    );
    const url3 = urlutil.expand("http://foo.bar/{?ipmask}", TEST_IMPRESSION("remoteIp", "  "));
    expect(url1).to.equal("http://foo.bar/?ipmask=127.0.0.0");
    expect(url2).to.equal("http://foo.bar/?ipmask=127.0.0.0");
    expect(url3).to.equal("http://foo.bar/");
  });

  it("only ipv4 addresses", () => {
    const url1 = urlutil.expand("http://foo.bar/{?ipv4}", TEST_IMPRESSION());
    const url2 = urlutil.expand(
      "http://foo.bar/{?ipv4}",
      TEST_IMPRESSION("remoteIp", "  what , 127.0.0.1"),
    );
    const url3 = urlutil.expand("http://foo.bar/{?ipv4}", TEST_IMPRESSION("remoteIp", "  "));
    const url4 = urlutil.expand(
      "http://foo.bar/{?ipv4}",
      TEST_IMPRESSION("remoteIp", "  what , 2804:18:1012:6b65::"),
    );
    expect(url1).to.equal("http://foo.bar/?ipv4=127.0.0.1");
    expect(url2).to.equal("http://foo.bar/?ipv4=127.0.0.1");
    expect(url3).to.equal("http://foo.bar/");
    expect(url4).to.equal("http://foo.bar/");
  });

  it("returns timestamps in milliseconds", () => {
    const url1 = urlutil.expand("http://foo.bar/{?timestamp}", TEST_IMPRESSION());
    const url2 = urlutil.expand(
      "http://foo.bar/{?timestamp}",
      TEST_IMPRESSION("timestamp", 1507234920010),
    );
    expect(url1).to.equal("http://foo.bar/?timestamp=1507234920000");
    expect(url2).to.equal("http://foo.bar/?timestamp=1507234920010");
  });

  it("does not collide on 32 bit random ints very often", () => {
    const many = Array(1000)
      .fill()
      .map(() => {
        return urlutil.expand("{randomint}", TEST_IMPRESSION("requestUuid", uuid.v4()));
      });
    expect(new Set(many).size).to.equal(1000);
    many.forEach((url) => {
      const num = parseInt(url, 10);
      const bitCount = num.toString(2).match(/1/g).length;
      expect(num).to.be.above(0);
      expect(num).to.be.at.most(2147483647);
      expect(bitCount).to.be.at.most(32);
    });
  });

  it("returns random strings based on timestamp + listenerepisode + ad", () => {
    const url1 = urlutil.expand("http://foo.bar/{?randomstr}", TEST_IMPRESSION());
    const url2 = urlutil.expand("http://foo.bar/{?randomstr}", TEST_IMPRESSION());
    const url3 = urlutil.expand(
      "http://foo.bar/{?randomstr}",
      TEST_IMPRESSION("timestamp", 9999999),
    );
    const url4 = urlutil.expand(
      "http://foo.bar/{?randomstr}",
      TEST_IMPRESSION("listenerEpisode", "changed"),
    );
    const url5 = urlutil.expand("http://foo.bar/{?randomstr}", TEST_IMPRESSION("adId", 8));
    expect(url1).to.equal(url2);
    expect(url1).not.to.equal(url3);
    expect(url1).not.to.equal(url4);
    expect(url1).not.to.equal(url5);
    expect(url3).not.to.equal(url4);
    expect(url4).not.to.equal(url5);
  });

  it("reassembles original request url", () => {
    const url = urlutil.expand("http://foo.bar/?ru={url}", TEST_IMPRESSION());
    expect(url).to.equal("http://foo.bar/?ru=dovetail.prxu.org%2F99%2Fthe%2Fpath.mp3%3Ffoo%3Dbar");
  });

  it("returns ad position data", () => {
    const tpl = "http://foo.bar{?totalduration,adpodposition,adpodoffsetstart,adposition}";
    const url = urlutil.expand(tpl, TEST_IMPRESSION());
    expect(url).to.include("totalduration=1458.491");
    expect(url).to.include("adpodposition=2");
    expect(url).to.include("adpodoffsetstart=897.06");
    expect(url).to.include("adposition=a");
  });

  it("counts by hostname", () => {
    expect(urlutil.count({}, null)).to.eql({});
    expect(urlutil.count({ start: 99 }, undefined)).to.eql({ start: 99 });
    expect(urlutil.count({}, "http://foo.gov/bar")).to.eql({ "foo.gov": 1 });
    expect(urlutil.count({ "foo.gov": 10 }, "https://foo.gov/bar")).to.eql({ "foo.gov": 11 });
  });
});
