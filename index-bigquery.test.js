import { jest } from "@jest/globals";
import log from "lambda-log";
import * as index from "./index-bigquery";
import { buildEvent } from "./test/support";
import testRecs from "./test/support/test-records";

describe("index-bigquery", () => {
  let inserts, insert, mockClient;
  beforeEach(() => {
    inserts = [];
    insert = jest.fn().mockImplementation(async (rows) => {
      inserts.push(rows);
      return rows.length;
    });
    mockClient = { dataset: () => ({ table: () => ({ insert }) }) };
  });

  describe(".handler", () => {
    it("inserts downloads and impressions", async () => {
      jest.spyOn(log, "info").mockReturnValue();

      const event = await buildEvent(testRecs);
      event.bigqueryClient = mockClient;

      await index.handler(event);
      expect(log.info.mock.calls.length).toEqual(2);
      expect(log.info.mock.calls[0][0]).toEqual("Starting BigQuery");
      expect(log.info.mock.calls[0][1]).toEqual({ records: 7, downloads: 1, impressions: 4 });
      expect(log.info.mock.calls[1][0]).toEqual("Finished BigQuery");
      expect(log.info.mock.calls[1][1]).toEqual({ records: 7, downloads: 1, impressions: 4 });

      expect(inserts[0].length).toEqual(1);
      expect(inserts[0][0].insertId).toEqual("listener-episode-1/1487703699");
      expect(inserts[0][0].json.timestamp).toEqual(1487703699);
      expect(inserts[0][0].json.request_uuid).toEqual("req-uuid");

      // all impressions should have different insert ids
      expect(inserts[1].length).toEqual(4);
      inserts[1].forEach((i) => {
        expect(i.insertId.length).toBeGreaterThan(10);
      });
      expect([...new Set(inserts[1].map((i) => i.insertId))].length).toEqual(4);

      const i1 = inserts[1].find((i) => i.json.ad_id === 12).json;
      expect(i1.timestamp).toEqual(1487703699);
      expect(i1.request_uuid).toEqual("req-uuid");
      expect(i1.segment).toEqual(0);
      expect(i1.is_confirmed).toEqual(false);
      expect(i1.is_duplicate).toEqual(false);
      expect(i1.cause).toBeNull();

      const i2 = inserts[1].find((i) => i.json.ad_id === 98).json;
      expect(i2.timestamp).toEqual(1487703699);
      expect(i2.request_uuid).toEqual("req-uuid");
      expect(i2.segment).toEqual(0);
      expect(i2.is_confirmed).toEqual(true);
      expect(i2.is_duplicate).toEqual(true);
      expect(i2.cause).toEqual("something");

      const i3 = inserts[1].find((i) => i.json.ad_id === 76).json;
      expect(i3.timestamp).toEqual(1487703699);
      expect(i3.request_uuid).toEqual("req-uuid");
      expect(i3.segment).toEqual(3);
      expect(i3.is_confirmed).toEqual(true);
      expect(i3.is_duplicate).toEqual(false);
      expect(i3.cause).toBeNull();

      const i4 = inserts[1].find((i) => i.json.ad_id === 104).json;
      expect(i4.timestamp).toEqual(1487703699);
      expect(i4.request_uuid).toEqual("req-uuid-vast");
      expect(i4.segment).toEqual(3);
      expect(i4.is_confirmed).toEqual(true);
      expect(i4.is_duplicate).toEqual(false);
      expect(i4.cause).toBeNull();
      expect(i4.vast_advertiser).toEqual("vastadvertiser1");
      expect(i4.vast_ad_id).toEqual("vastad1");
      expect(i4.vast_creative_id).toEqual("vastcreative1");
      expect(i4.vast_price_value).toEqual(10.0);
      expect(i4.vast_price_currency).toEqual("USD");
      expect(i4.vast_price_model).toEqual("CPM");
    });

    it("ignores unknown types", async () => {
      jest.spyOn(log, "info").mockReturnValue();

      const event = await buildEvent([
        { type: "postbytes", timestamp: 1 },
        { type: "postbytes", timestamp: 1, impressions: [] },
        { type: "bytes", timestamp: 1, download: {}, impressions: [{}] },
        { type: "whatev", timestamp: 1, download: {}, impressions: [{}] },
      ]);
      event.bigqueryClient = mockClient;

      await index.handler(event);
      expect(inserts.length).toEqual(0);
      expect(log.info.mock.calls.length).toEqual(2);
      expect(log.info.mock.calls[0][0]).toEqual("Starting BigQuery");
      expect(log.info.mock.calls[0][1]).toEqual({ records: 4, downloads: 0, impressions: 0 });
      expect(log.info.mock.calls[1][0]).toEqual("Finished BigQuery");
      expect(log.info.mock.calls[1][1]).toEqual({ records: 4, downloads: 0, impressions: 0 });
    });
  });

  describe(".format", () => {
    it("formats common fields", () => {
      const data = index.format({
        agentName: 11,
        agentOs: 22,
        agentType: 33,
        city: 44,
        country: 55,
        digest: "the-digest",
        feederEpisode: "the-ep",
        feederFeed: "the-feed",
        feederPodcast: 1234,
        listenerId: "the-listener",
        postalCode: "the-postal",
        requestUuid: "abcd1234",
        timestamp: 12345678,
      });

      expect(data.agent_name_id).toEqual(11);
      expect(data.agent_os_id).toEqual(22);
      expect(data.agent_type_id).toEqual(33);
      expect(data.city_geoname_id).toEqual(44);
      expect(data.country_geoname_id).toEqual(55);
      expect(data.digest).toEqual("the-digest");
      expect(data.feeder_episode).toEqual("the-ep");
      expect(data.feeder_feed).toEqual("the-feed");
      expect(data.feeder_podcast).toEqual(1234);
      expect(data.is_confirmed).toEqual(false);
      expect(data.listener_id).toEqual("the-listener");
      expect(data.postal_code).toEqual("the-postal");
      expect(data.request_uuid).toEqual("abcd1234");
      expect(data.timestamp).toEqual(12345678);
    });

    it("handles epoch seconds and milliseconds", () => {
      expect(index.format({}).timestamp).toEqual(0);
      expect(index.format({ timestamp: 1758811111 }).timestamp).toEqual(1758811111);
      expect(index.format({ timestamp: 1758811111000 }).timestamp).toEqual(1758811111);
    });
  });

  describe(".formatDownload", () => {
    it("formats download data", () => {
      const data = index.formatDownload({
        download: { isDuplicate: true, cause: "the-cause", adCount: 11 },
        listenerEpisode: "the-le",
        url: "the-url",
        remoteReferrer: "the-ref",
        remoteAgent: "the-agent",
        remoteIp: "12.34.56.78",
        filled: { paid: [1, 2, 3], house: [4, 5, 6] },
        unfilled: { paid: [9, 8, 7], house: [6, 5, 4] },
      });

      expect(data.json.ad_count).toEqual(11);
      expect(data.json.is_duplicate).toEqual(true);
      expect(data.json.cause).toEqual("the-cause");
      expect(data.json.listener_episode).toEqual("the-le");
      expect(data.json.url).toEqual("the-url");
      expect(data.json.remote_referrer).toEqual("the-ref");
      expect(data.json.remote_agent).toEqual("the-agent");
      expect(data.json.remote_ip).toEqual("12.34.56.0");
      expect(data.json.zones_filled_pre).toEqual(1);
      expect(data.json.zones_filled_mid).toEqual(2);
      expect(data.json.zones_filled_post).toEqual(3);
      expect(data.json.zones_filled_house_pre).toEqual(4);
      expect(data.json.zones_filled_house_mid).toEqual(5);
      expect(data.json.zones_filled_house_post).toEqual(6);
      expect(data.json.zones_unfilled_pre).toEqual(9);
      expect(data.json.zones_unfilled_mid).toEqual(8);
      expect(data.json.zones_unfilled_post).toEqual(7);
      expect(data.json.zones_unfilled_house_pre).toEqual(6);
      expect(data.json.zones_unfilled_house_mid).toEqual(5);
      expect(data.json.zones_unfilled_house_post).toEqual(4);
    });

    it("produces unique insert ids", () => {
      const d1 = index.formatDownload({ timestamp: 12345678, listenerEpisode: "le" });
      const d2 = index.formatDownload({ timestamp: 12345678, listenerEpisode: "le" });
      const d3 = index.formatDownload({ timestamp: 1234567, listenerEpisode: "le" });
      const d4 = index.formatDownload({ timestamp: 12345678, listenerEpisode: "le2" });

      expect(d1.insertId).toEqual(d2.insertId);
      expect(d1.insertId).not.toEqual(d3.insertId);
      expect(d1.insertId).not.toEqual(d4.insertId);
      expect(d2.insertId).not.toEqual(d4.insertId);
    });

    it("always sets duplicate boolean", () => {
      const checkDup = (d) => index.formatDownload(d).json.is_duplicate;

      expect(checkDup({})).toEqual(false);
      expect(checkDup({ download: {} })).toEqual(false);
      expect(checkDup({ download: { isDuplicate: null } })).toEqual(false);
      expect(checkDup({ download: { isDuplicate: false } })).toEqual(false);
      expect(checkDup({ download: { isDuplicate: true } })).toEqual(true);
      expect(checkDup({ download: { isDuplicate: "truthy" } })).toEqual(true);
    });

    it("cleans and masks IP addresses", () => {
      const checkIp = (d) => index.formatDownload(d).json.remote_ip;

      expect(checkIp({ remoteIp: "12.34.56.7" })).toEqual("12.34.56.0");
      expect(checkIp({ remoteIp: "1:2:3:4::5:6:7:8" })).toEqual("1:2:3:4::");
      expect(checkIp({ remoteIp: " 1.2.3.4, 5.6.7.8 " })).toEqual("1.2.3.0");
      expect(checkIp({ remoteIp: " foo, 5.6.7.8 " })).toEqual("5.6.7.0");
    });
  });

  describe(".formatImpression", () => {
    it("formats impressions data", () => {
      const data = index.formatImpression([
        {},
        {
          adId: 11,
          campaignId: 22,
          cause: "the-cause",
          creativeId: 33,
          flightId: 44,
          isDuplicate: true,
          placementsKey: "the-key",
          segment: 55,
          targetPath: "the-path",
          zoneName: "the-zone",
          vast: {
            advertiser: "the-adv",
            ad: { id: "the-ad" },
            creative: { id: "the-creative" },
            pricing: {
              value: 12.3,
              currency: "the-curr",
              model: "the-model",
            },
          },
        },
      ]);

      expect(data.json.ad_id).toEqual(11);
      expect(data.json.campaign_id).toEqual(22);
      expect(data.json.cause).toEqual("the-cause");
      expect(data.json.creative_id).toEqual(33);
      expect(data.json.flight_id).toEqual(44);
      expect(data.json.is_duplicate).toEqual(true);
      expect(data.json.placements_key).toEqual("the-key");
      expect(data.json.segment).toEqual(55);
      expect(data.json.target_path).toEqual("the-path");
      expect(data.json.zone_name).toEqual("the-zone");
      expect(data.json.vast_advertiser).toEqual("the-adv");
      expect(data.json.vast_ad_id).toEqual("the-ad");
      expect(data.json.vast_creative_id).toEqual("the-creative");
      expect(data.json.vast_price_value).toEqual(12.3);
      expect(data.json.vast_price_currency).toEqual("the-curr");
      expect(data.json.vast_price_model).toEqual("the-model");
    });

    it("always sets duplicate boolean", () => {
      expect(index.formatImpression([{}, {}]).json.is_duplicate).toEqual(false);
      expect(index.formatImpression([{}, { isDuplicate: false }]).json.is_duplicate).toEqual(false);
      expect(index.formatImpression([{}, { isDuplicate: true }]).json.is_duplicate).toEqual(true);
      expect(index.formatImpression([{}, { isDuplicate: "t" }]).json.is_duplicate).toEqual(true);
    });

    it("parses vast pricing floats", () => {
      const checkPrice = (i) => index.formatImpression([{}, i]).json.vast_price_value;

      expect(checkPrice({})).toBeNull();
      expect(checkPrice({ vast: { pricing: { value: "" } } })).toBeNull();
      expect(checkPrice({ vast: { pricing: { value: "1.23" } } })).toEqual(1.23);
      expect(checkPrice({ vast: { pricing: { value: 1.23 } } })).toEqual(1.23);
    });
  });
});
