'use strict';

const support = require('./support');
const AdPosition = require('../lib/ad-position');

describe('AdPosition', () => {
  const durations = [24.03, 32.31, 840.72, 19.96, 495.51, 39.761, 6.2];
  const types = 'aaoaohi';
  const segment = 3;

  let ad;
  beforeEach(() => (ad = new AdPosition({ durations, types, segment })));

  it('#totalDuration', () => {
    expect(ad.totalDuration()).to.equal(1458.491);
  });

  it('#totalAdDuration', () => {
    expect(ad.totalAdDuration()).to.equal(116.061);

    ad = new AdPosition({ durations, segment, types: 'biohooi' });
    expect(ad.totalAdDuration()).to.equal(43.99);
  });

  it('#totalAdPods', () => {
    expect(ad.totalAdPods()).to.equal(3);

    ad = new AdPosition({ durations, segment, types: 'oaoihai' });
    expect(ad.totalAdPods()).to.equal(2);

    ad = new AdPosition({ durations, segment, types: 'oaoihia' });
    expect(ad.totalAdPods()).to.equal(3);

    ad = new AdPosition({ durations, segment, types: 'ooohaai' });
    expect(ad.totalAdPods()).to.equal(1);
  });

  it('#adPodPosition', () => {
    expect(ad.adPodPosition()).to.equal(2);

    expect(ad.adPodPosition(0)).to.equal(1);
    expect(ad.adPodPosition(1)).to.equal(1);
    expect(ad.adPodPosition(2)).to.be.undefined;
    expect(ad.adPodPosition(3)).to.equal(2);
    expect(ad.adPodPosition(4)).to.be.undefined;
    expect(ad.adPodPosition(5)).to.equal(3);
    expect(ad.adPodPosition(6)).to.be.undefined;

    ad = new AdPosition({ durations, types: 'oaoihia' });
    expect(ad.adPodPosition(6)).to.equal(3);
  });

  it('#adPodOffsetStart', () => {
    expect(ad.adPodOffsetStart()).to.equal(897.06);

    expect(ad.adPodOffsetStart(0)).to.equal(0);
    expect(ad.adPodOffsetStart(1)).to.equal(0);
    expect(ad.adPodOffsetStart(2)).to.be.undefined;
    expect(ad.adPodOffsetStart(3)).to.equal(897.06);
    expect(ad.adPodOffsetStart(4)).to.be.undefined;
    expect(ad.adPodOffsetStart(5)).to.equal(1412.53);
    expect(ad.adPodOffsetStart(6)).to.be.undefined;
  });

  it('#adPodOffsetPrevious', () => {
    expect(ad.adPodOffsetPrevious()).to.equal(840.72);

    expect(ad.adPodOffsetPrevious(0)).to.be.undefined;
    expect(ad.adPodOffsetPrevious(1)).to.be.undefined;
    expect(ad.adPodOffsetPrevious(2)).to.be.undefined;
    expect(ad.adPodOffsetPrevious(3)).to.equal(840.72);
    expect(ad.adPodOffsetPrevious(4)).to.be.undefined;
    expect(ad.adPodOffsetPrevious(5)).to.equal(495.51);
    expect(ad.adPodOffsetPrevious(6)).to.be.undefined;

    ad = new AdPosition({ durations, types: 'oaoihia' });
    expect(ad.adPodOffsetPrevious(1)).to.be.undefined;
    expect(ad.adPodOffsetPrevious(4)).to.equal(860.68);
    expect(ad.adPodOffsetPrevious(6)).to.equal(39.761);
  });

  it('#adPodOffsetNext', () => {
    expect(ad.adPodOffsetNext()).to.equal(495.51);

    expect(ad.adPodOffsetNext(0)).to.equal(840.72);
    expect(ad.adPodOffsetNext(1)).to.equal(840.72);
    expect(ad.adPodOffsetNext(2)).to.be.undefined;
    expect(ad.adPodOffsetNext(3)).to.equal(495.51);
    expect(ad.adPodOffsetNext(4)).to.be.undefined;
    expect(ad.adPodOffsetNext(5)).to.be.undefined;
    expect(ad.adPodOffsetNext(6)).to.be.undefined;

    ad = new AdPosition({ durations, types: 'oaoihia' });
    expect(ad.adPodOffsetNext(1)).to.equal(860.68);
    expect(ad.adPodOffsetNext(4)).to.equal(39.761);
    expect(ad.adPodOffsetNext(6)).to.be.undefined;
  });

  it('#adPodDuration', () => {
    expect(ad.adPodDuration()).to.equal(19.96);

    expect(ad.adPodDuration(0)).to.equal(56.34);
    expect(ad.adPodDuration(1)).to.equal(56.34);
    expect(ad.adPodDuration(2)).to.be.undefined;
    expect(ad.adPodDuration(3)).to.equal(19.96);
    expect(ad.adPodDuration(4)).to.be.undefined;
    expect(ad.adPodDuration(5)).to.equal(39.761);
    expect(ad.adPodDuration(6)).to.be.undefined;

    ad = new AdPosition({ durations, types: 'oaoihia' });
    expect(ad.adPodDuration(1)).to.equal(32.31);
    expect(ad.adPodDuration(4)).to.equal(495.51);
    expect(ad.adPodDuration(6)).to.equal(6.2);
  });

  it('#adPosition', () => {
    expect(ad.adPosition()).to.equal('a');

    expect(ad.adPosition(0)).to.equal('a');
    expect(ad.adPosition(1)).to.equal('b');
    expect(ad.adPosition(2)).to.be.undefined;
    expect(ad.adPosition(3)).to.equal('a');
    expect(ad.adPosition(4)).to.be.undefined;
    expect(ad.adPosition(5)).to.equal('a');
    expect(ad.adPosition(6)).to.be.undefined;

    ad = new AdPosition({ durations, types: 'oah?hia' });
    expect(ad.adPosition(1)).to.equal('a');
    expect(ad.adPosition(2)).to.equal('b');
    expect(ad.adPosition(3)).to.equal('c');
    expect(ad.adPosition(4)).to.equal('d');
    expect(ad.adPosition(6)).to.equal('a');
  });

  it('#adPositionOffset', () => {
    expect(ad.adPositionOffset()).to.equal(0);

    expect(ad.adPositionOffset(0)).to.equal(0);
    expect(ad.adPositionOffset(1)).to.equal(24.03);
    expect(ad.adPositionOffset(2)).to.be.undefined;
    expect(ad.adPositionOffset(3)).to.equal(0);
    expect(ad.adPositionOffset(4)).to.be.undefined;
    expect(ad.adPositionOffset(5)).to.equal(0);
    expect(ad.adPositionOffset(6)).to.be.undefined;

    ad = new AdPosition({ durations, types: 'oah?hia' });
    expect(ad.adPositionOffset(1)).to.equal(0);
    expect(ad.adPositionOffset(2)).to.equal(32.31);
    expect(ad.adPositionOffset(3)).to.equal(873.03);
    expect(ad.adPositionOffset(4)).to.equal(892.99);
    expect(ad.adPositionOffset(6)).to.equal(0);
  });
});
