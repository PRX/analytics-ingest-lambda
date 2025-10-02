import { jest } from "@jest/globals";
import log from "lambda-log";
import { v4 } from "uuid";
import { handler } from "../index-bigquery";
import { client } from "../lib/bigquery";
import { buildEvent } from "./support";

describe("bigquery integration", () => {
  beforeEach(() => {
    if (!process.env.BQ_CREDENTIALS) {
      throw new Error("You must set BQ_CREDENTIALS");
    }
    if (!process.env.BQ_DATASET) {
      throw new Error("You must set BQ_DATASET");
    }
    if (["production", "staging"].includes(process.env.BQ_DATASET)) {
      throw new Error("Not allowed to use prod/stag BQ_DATASET");
    }
  });

  it("inserts downloads and impressions", async () => {
    jest.spyOn(log, "info").mockReturnValue();

    const recs = [
      {
        type: "postbytes",
        timestamp: Date.now(),
        requestUuid: v4(),
        feederPodcast: 1234,
        feederEpisode: v4(),
        digest: "the-digest",
        listenerId: v4(),
        listenerEpisode: v4(),
        remoteIp: "12.34.56.78",
        unfilled: { house: [9, 8, 7] },
        download: { adCount: 2 },
        impressions: [
          { isDuplicate: true, cause: "stuff", segment: 1, flightId: 88 },
          { segment: 3, flightId: 99 },
        ],
      },
    ];

    await handler(await buildEvent(recs));
    expect(log.info.mock.calls.length).toEqual(2);
    expect(log.info.mock.calls[0][1]).toEqual({ records: 1, downloads: 1, impressions: 2 });
    expect(log.info.mock.calls[1][1]).toEqual({ records: 1, downloads: 1, impressions: 2 });

    const bigquery = await client();
    const q1 = `SELECT * FROM ${process.env.BQ_DATASET}.dt_downloads WHERE timestamp > @ts AND request_uuid = @id`;
    const q2 = `SELECT * FROM ${process.env.BQ_DATASET}.dt_impressions WHERE timestamp > @ts AND request_uuid = @id ORDER BY segment ASC`;
    const params = { ts: new Date(recs[0].timestamp - 1000), id: recs[0].requestUuid };
    const [downs] = await bigquery.query({ query: q1, params });
    const [imps] = await bigquery.query({ query: q2, params });

    expect(downs.length).toEqual(1);
    expect(imps.length).toEqual(2);

    // re-parse timestamp strings to compare with ours - should be millisecond precision
    expect(Date.parse(downs[0].timestamp.value).valueOf()).toEqual(recs[0].timestamp);
    expect(Date.parse(imps[0].timestamp.value).valueOf()).toEqual(recs[0].timestamp);
    expect(Date.parse(imps[1].timestamp.value).valueOf()).toEqual(recs[0].timestamp);

    // common fields
    for (const rec of downs.concat(imps)) {
      expect(rec.request_uuid).toEqual(recs[0].requestUuid);
      expect(rec.feeder_podcast).toEqual(recs[0].feederPodcast);
      expect(rec.feeder_episode).toEqual(recs[0].feederEpisode);
      expect(rec.digest).toEqual(recs[0].digest);
      expect(rec.listener_id).toEqual(recs[0].listenerId);
    }

    // downloads
    expect(downs[0].listener_episode).toEqual(recs[0].listenerEpisode);
    expect(downs[0].remote_ip).toEqual("12.34.56.0");
    expect(downs[0].zones_unfilled_house_pre).toEqual(9);
    expect(downs[0].zones_unfilled_house_mid).toEqual(8);
    expect(downs[0].zones_unfilled_house_post).toEqual(7);
    expect(downs[0].ad_count).toEqual(2);
    expect(downs[0].is_duplicate).toEqual(false);

    // impressions
    expect(imps[0].segment).toEqual(1);
    expect(imps[0].flight_id).toEqual(88);
    expect(imps[0].is_duplicate).toEqual(true);
    expect(imps[0].cause).toEqual("stuff");
    expect(imps[1].segment).toEqual(3);
    expect(imps[1].flight_id).toEqual(99);
    expect(imps[1].is_duplicate).toEqual(false);
  });
});
