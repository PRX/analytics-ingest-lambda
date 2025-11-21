import AdPosition from "./ad-position";

describe("AdPosition", () => {
  const durations = [24.03, 32.31, 840.72, 19.96, 495.51, 39.761, 6.2];
  const types = "aaoaohi";
  const segment = 3;

  let ad;
  beforeEach(() => {
    ad = new AdPosition({ durations, types, segment });
  });

  it("#totalDuration", () => {
    expect(ad.totalDuration()).toEqual(1458.491);
  });

  it("#totalAdDuration", () => {
    expect(ad.totalAdDuration()).toEqual(116.061);

    ad = new AdPosition({ durations, segment, types: "biohooi" });
    expect(ad.totalAdDuration()).toEqual(43.99);
  });

  it("#totalAdPods", () => {
    expect(ad.totalAdPods()).toEqual(3);

    ad = new AdPosition({ durations, segment, types: "oaoihai" });
    expect(ad.totalAdPods()).toEqual(2);

    ad = new AdPosition({ durations, segment, types: "oaoihia" });
    expect(ad.totalAdPods()).toEqual(3);

    ad = new AdPosition({ durations, segment, types: "ooohaai" });
    expect(ad.totalAdPods()).toEqual(1);
  });

  it("#adPodPosition", () => {
    expect(ad.adPodPosition()).toEqual(2);

    expect(ad.adPodPosition(0)).toEqual(1);
    expect(ad.adPodPosition(1)).toEqual(1);
    expect(ad.adPodPosition(2)).toBeUndefined();
    expect(ad.adPodPosition(3)).toEqual(2);
    expect(ad.adPodPosition(4)).toBeUndefined();
    expect(ad.adPodPosition(5)).toEqual(3);
    expect(ad.adPodPosition(6)).toBeUndefined();

    ad = new AdPosition({ durations, types: "oaoihia" });
    expect(ad.adPodPosition(6)).toEqual(3);
  });

  it("#adPodOffsetStart", () => {
    expect(ad.adPodOffsetStart()).toEqual(897.06);

    expect(ad.adPodOffsetStart(0)).toEqual(0);
    expect(ad.adPodOffsetStart(1)).toEqual(0);
    expect(ad.adPodOffsetStart(2)).toBeUndefined();
    expect(ad.adPodOffsetStart(3)).toEqual(897.06);
    expect(ad.adPodOffsetStart(4)).toBeUndefined();
    expect(ad.adPodOffsetStart(5)).toEqual(1412.53);
    expect(ad.adPodOffsetStart(6)).toBeUndefined();
  });

  it("#adPodOffsetPrevious", () => {
    expect(ad.adPodOffsetPrevious()).toEqual(840.72);

    expect(ad.adPodOffsetPrevious(0)).toBeUndefined();
    expect(ad.adPodOffsetPrevious(1)).toBeUndefined();
    expect(ad.adPodOffsetPrevious(2)).toBeUndefined();
    expect(ad.adPodOffsetPrevious(3)).toEqual(840.72);
    expect(ad.adPodOffsetPrevious(4)).toBeUndefined();
    expect(ad.adPodOffsetPrevious(5)).toEqual(495.51);
    expect(ad.adPodOffsetPrevious(6)).toBeUndefined();

    ad = new AdPosition({ durations, types: "oaoihia" });
    expect(ad.adPodOffsetPrevious(1)).toBeUndefined();
    expect(ad.adPodOffsetPrevious(4)).toEqual(860.68);
    expect(ad.adPodOffsetPrevious(6)).toEqual(39.761);
  });

  it("#adPodOffsetNext", () => {
    expect(ad.adPodOffsetNext()).toEqual(495.51);

    expect(ad.adPodOffsetNext(0)).toEqual(840.72);
    expect(ad.adPodOffsetNext(1)).toEqual(840.72);
    expect(ad.adPodOffsetNext(2)).toBeUndefined();
    expect(ad.adPodOffsetNext(3)).toEqual(495.51);
    expect(ad.adPodOffsetNext(4)).toBeUndefined();
    expect(ad.adPodOffsetNext(5)).toBeUndefined();
    expect(ad.adPodOffsetNext(6)).toBeUndefined();

    ad = new AdPosition({ durations, types: "oaoihia" });
    expect(ad.adPodOffsetNext(1)).toEqual(860.68);
    expect(ad.adPodOffsetNext(4)).toEqual(39.761);
    expect(ad.adPodOffsetNext(6)).toBeUndefined();
  });

  it("#adPodDuration", () => {
    expect(ad.adPodDuration()).toEqual(19.96);

    expect(ad.adPodDuration(0)).toEqual(56.34);
    expect(ad.adPodDuration(1)).toEqual(56.34);
    expect(ad.adPodDuration(2)).toBeUndefined();
    expect(ad.adPodDuration(3)).toEqual(19.96);
    expect(ad.adPodDuration(4)).toBeUndefined();
    expect(ad.adPodDuration(5)).toEqual(39.761);
    expect(ad.adPodDuration(6)).toBeUndefined();

    ad = new AdPosition({ durations, types: "oaoihia" });
    expect(ad.adPodDuration(1)).toEqual(32.31);
    expect(ad.adPodDuration(4)).toEqual(495.51);
    expect(ad.adPodDuration(6)).toEqual(6.2);
  });

  it("#adPosition", () => {
    expect(ad.adPosition()).toEqual("a");

    expect(ad.adPosition(0)).toEqual("a");
    expect(ad.adPosition(1)).toEqual("b");
    expect(ad.adPosition(2)).toBeUndefined();
    expect(ad.adPosition(3)).toEqual("a");
    expect(ad.adPosition(4)).toBeUndefined();
    expect(ad.adPosition(5)).toEqual("a");
    expect(ad.adPosition(6)).toBeUndefined();

    ad = new AdPosition({ durations, types: "oah?hia" });
    expect(ad.adPosition(1)).toEqual("a");
    expect(ad.adPosition(2)).toEqual("b");
    expect(ad.adPosition(3)).toEqual("c");
    expect(ad.adPosition(4)).toEqual("d");
    expect(ad.adPosition(6)).toEqual("a");
  });

  it("#adPositionOffset", () => {
    expect(ad.adPositionOffset()).toEqual(0);

    expect(ad.adPositionOffset(0)).toEqual(0);
    expect(ad.adPositionOffset(1)).toEqual(24.03);
    expect(ad.adPositionOffset(2)).toBeUndefined();
    expect(ad.adPositionOffset(3)).toEqual(0);
    expect(ad.adPositionOffset(4)).toBeUndefined();
    expect(ad.adPositionOffset(5)).toEqual(0);
    expect(ad.adPositionOffset(6)).toBeUndefined();

    ad = new AdPosition({ durations, types: "oah?hia" });
    expect(ad.adPositionOffset(1)).toEqual(0);
    expect(ad.adPositionOffset(2)).toEqual(32.31);
    expect(ad.adPositionOffset(3)).toEqual(873.03);
    expect(ad.adPositionOffset(4)).toEqual(892.99);
    expect(ad.adPositionOffset(6)).toEqual(0);
  });
});
