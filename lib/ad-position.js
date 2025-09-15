// javascript is bad at math
const ROUND_TO = 6;
const ROUND_FACTOR = 10 ** (ROUND_TO - 1);
const round = (num) => Math.round((num + Number.EPSILON) * ROUND_FACTOR) / ROUND_FACTOR;
const sum = (arr) => round(arr.reduce((acc, n) => acc + n, 0));

// original segments and sonic_id ads are both "original content"
const isOriginal = (type) => type === "o" || type === "i";
const isAd = (type) => type && !isOriginal(type);

/**
 * Utility for calculating ad positioning data, using the durations and
 * types of segments in the file.
 */
module.exports = class AdPosition {
  constructor(record = {}) {
    this.durations = record.durations;
    this.types = record.types;
    this.segment = record.segment;

    // check for good data
    this.okay = this.durations?.length > 0 && this.types?.length === this.durations.length;
    this.okayAd = this.okay && isAd(this.types[this.segment]);

    // calculate the 1-based pod position for all ads
    this.pods = Array(this.types?.length || 0).fill();
    for (let i = 0, n = 1; i < this.pods.length; i++) {
      if (isAd(this.types[i])) {
        this.pods[i] = n;
        if (isOriginal(this.types[i + 1])) {
          n++;
        }
      }
    }
  }

  // Total file duration, including ads
  totalDuration() {
    if (this.okay) {
      return sum(this.durations);
    }
  }

  // Duration of all ad content in this specific download
  totalAdDuration() {
    if (this.okay) {
      return sum(this.durations.filter((_d, i) => isAd(this.types[i])));
    }
  }

  // Number of ad pods in the file
  totalAdPods() {
    if (this.okay) {
      return new Set(this.pods.filter((n) => n)).size;
    }
  }

  // Number designating which ad pod this ad is part of
  adPodPosition(index = this.segment) {
    if (this.okay && isAd(this.types[index])) {
      return this.pods[index];
    }
  }

  // Seconds offset this ad pod is from the start of the file
  adPodOffsetStart(index = this.segment) {
    if (this.okay && isAd(this.types[index])) {
      const podPosition = this.pods[index];
      const podStartIndex = this.pods.indexOf(podPosition);
      return sum(this.durations.slice(0, podStartIndex));
    }
  }

  // Seconds offset this pod is from the previous pod
  adPodOffsetPrevious(index = this.segment) {
    if (this.okay && isAd(this.types[index])) {
      const podPosition = this.pods[index];
      const podStartIndex = this.pods.indexOf(podPosition);
      const prevPodEndIndex = this.pods.lastIndexOf(podPosition - 1);
      if (prevPodEndIndex > -1) {
        return sum(this.durations.slice(prevPodEndIndex + 1, podStartIndex));
      }
    }
  }

  // Seconds offset this pod is from the next pod
  adPodOffsetNext(index = this.segment) {
    if (this.okay && isAd(this.types[index])) {
      const podPosition = this.pods[index];
      const podEndIndex = this.pods.lastIndexOf(podPosition);
      const nextPodStartIndex = this.pods.indexOf(podPosition + 1);
      if (nextPodStartIndex > -1) {
        return sum(this.durations.slice(podEndIndex + 1, nextPodStartIndex));
      }
    }
  }

  // Duration of just this ad pod
  adPodDuration(index = this.segment) {
    if (this.okay && isAd(this.types[index])) {
      const podPosition = this.pods[index];
      const podStartIndex = this.pods.indexOf(podPosition);
      const podEndIndex = this.pods.lastIndexOf(podPosition);
      return sum(this.durations.slice(podStartIndex, podEndIndex + 1));
    }
  }

  // Letter designating the order of where the specific ad is within a pod
  adPosition(index = this.segment) {
    if (this.okay && isAd(this.types[index])) {
      const podPosition = this.pods[index];
      const podStartIndex = this.pods.indexOf(podPosition);
      return String.fromCharCode(97 + (index - podStartIndex));
    }
  }

  // Seconds offset this ad is from the start of the pod
  adPositionOffset(index = this.segment) {
    if (this.okay && isAd(this.types[index])) {
      const podPosition = this.pods[index];
      const podStartIndex = this.pods.indexOf(podPosition);
      return sum(this.durations.slice(podStartIndex, index));
    }
  }
};
