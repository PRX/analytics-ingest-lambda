import { promisify } from "node:util";
import { gzip } from "node:zlib";

const gzipPromise = promisify(gzip);

// lambda log line prefixes
const prefixOld = "2021-06-30T19:51:13.886Z\tee30dae3-f25c-4a22-84e8-8b7b378215fb\t";
const prefixNew = "2021-06-30T19:51:13.886Z\tee30dae3-f25c-4a22-84e8-8b7b378215fb\tINFO\t";

/**
 * Encode records into any kinesis format (shouldn't matter), and return a handler event
 */
export async function buildEvent(recs) {
  const builders = [buildJsonRecord, buildLogRecord, buildLambdaRecord];
  const Records = await Promise.all(
    recs.map((r) => builders[Math.floor(Math.random() * builders.length)](r)),
  );
  return { Records };
}

/**
 * Plain base64'd json kinesis events, from direct SDK putRecords
 */
export async function buildJsonRecord(rec = {}) {
  const data = Buffer.from(JSON.stringify(rec), "utf-8").toString("base64");
  return { kinesis: { data } };
}

/**
 * Zipped json, from cloudwatch log subscription filters
 */
export async function buildLogRecord(recs = []) {
  recs = Array.isArray(recs) ? recs : [recs];
  const logEvents = recs.map((r) => ({ message: JSON.stringify(r) }));
  const zipped = await gzipPromise(JSON.stringify({ logEvents }));
  const data = Buffer.from(zipped).toString("base64");
  return { kinesis: { data } };
}

/**
 * Zipped json, from lambdas (which include some prefix lines)
 */
export async function buildLambdaRecord(recs = [], useOldPrefix = false) {
  recs = Array.isArray(recs) ? recs : [recs];
  const prefix = useOldPrefix ? prefixOld : prefixNew;
  const logEvents = recs.map((r) => ({ message: `${prefix + JSON.stringify(r)}\n` }));
  const zipped = await gzipPromise(JSON.stringify({ logEvents }));
  const data = Buffer.from(zipped).toString("base64");
  return { kinesis: { data } };
}
