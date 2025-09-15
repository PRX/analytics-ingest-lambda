// build a base64-encoded kinesis record
exports.buildRecord = (record) => {
  return {
    eventSource: "aws:kinesis",
    eventVersion: "1.0",
    kinesis: {
      data: Buffer.from(JSON.stringify(record), "utf-8").toString("base64"),
    },
  };
};

// build an event of multiple kinesis records
exports.buildEvent = (records) => {
  return {
    Records: records.map((r) => exports.buildRecord(r)),
  };
};

// build an event from input style and kinesis style recs
exports.buildMixedStyleEvent = (inputStyleRecords, kinesisStyleRecords) => {
  const res = {
    // Here we process the input style records, turning them into kinesis style
    // recs.
    // Just pass the kinesisStyleRecords through.
    Records: inputStyleRecords.map((r) => exports.buildRecord(r)).concat(kinesisStyleRecords),
  };
  return res;
};
