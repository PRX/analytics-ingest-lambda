import { promisify } from "node:util";
import { deflate as zdeflate, inflate as zinflate } from "node:zlib";
import { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { AssumeRoleCommand, STSClient } from "@aws-sdk/client-sts";
import log from "lambda-log";

const OPTIONS = {
  region: process.env.AWS_REGION || "us-east-1",
  httpOptions: { connectTimeout: 1000, timeout: 2000 },
  maxRetries: 3,
};

const pdeflate = promisify(zdeflate);
const pinflate = promisify(zinflate);
export const deflate = async (obj) => pdeflate(JSON.stringify(obj));
export const inflate = async (buff) => JSON.parse(await pinflate(buff));

/**
 * Create a DynamoDB client, optionally assuming a role for accessing DDB tables
 * in a different AWS account than the lambda.
 */
export async function client(RoleArn = process.env.DDB_ROLE) {
  if (RoleArn) {
    const RoleSessionName = "analytics-ingest-lambda-dynamodb";
    const stsClient = new STSClient(OPTIONS);
    const command = new AssumeRoleCommand({ RoleArn, RoleSessionName });

    try {
      const data = await stsClient.send(command);
      const { AccessKeyId, SecretAccessKey, SessionToken } = data.Credentials;
      return new DynamoDBClient({
        ...OPTIONS,
        credentials: {
          accessKeyId: AccessKeyId,
          secretAccessKey: SecretAccessKey,
          sessionToken: SessionToken,
        },
      });
    } catch (err) {
      log.error("DynamoDB STS error", { error: err, RoleArn, RoleSessionName });
      return new DynamoDBClient(OPTIONS);
    }
  } else {
    return new DynamoDBClient(OPTIONS);
  }
}

/**
 * Store both redirect data (from Dovetail Router) and segment data (from dovetail-counts-lambda)
 */
const getClient = client;
export async function upsertRedirect({ client, id, payload, segments, extras } = {}) {
  const params = {
    AttributeUpdates: {},
    Key: { id: { S: id } },
    ReturnValues: "ALL_OLD",
    TableName: process.env.DDB_TABLE,
  };

  // compress payload data
  if (payload) {
    params.AttributeUpdates.payload = {
      Action: "PUT",
      Value: { B: await deflate(payload) },
    };
  }

  // stringify segments array
  if (segments?.length) {
    params.AttributeUpdates.segments = {
      Action: "ADD",
      Value: { SS: (segments || []).map((s) => s.toString()) },
    };
  }

  // optional extra positioning data about the segments
  if (extras) {
    params.AttributeUpdates.extras = {
      Action: "PUT",
      Value: { S: JSON.stringify(extras) },
    };
  }

  // optional expiration
  const ttl = +process.env.DDB_TTL;
  if (ttl > 0) {
    const expires = Math.round(Date.now() / 1000) + ttl;
    params.AttributeUpdates.expiration = {
      Action: "PUT",
      Value: { N: expires.toString() },
    };
  }

  const command = new UpdateItemCommand(params);
  const result = await (client || (await getClient())).send(command);
  return upsertRedirectResult({ id, payload, segments, extras, result });
}

/**
 * Return the id, payload, and any "new" segments we just set
 */
export async function upsertRedirectResult({ id, payload, segments, extras, result }) {
  const oldPayload = result?.Attributes?.payload ? result?.Attributes.payload.B : null;
  const oldSegments = result?.Attributes?.segments ? result?.Attributes.segments.SS : [];
  const oldExtras = result?.Attributes?.extras ? result?.Attributes.extras.S : null;

  // only inflate the payload if necessary
  const newPayload = payload || (oldPayload && (await inflate(oldPayload))) || null;

  // merge extras into the payload
  if (newPayload && extras) {
    Object.assign(newPayload, extras);
  } else if (newPayload && oldExtras) {
    try {
      Object.assign(newPayload, JSON.parse(oldExtras));
    } catch (err) {
      log.warn(`Error json parsing old extras`, { error: err, extras: oldExtras });
    }
  }

  // sanitize the segments we just set
  const setSegments = (segments || []).map((s) => s.toString());

  // segments are "new" the first time they're set AND we have a non-null payload
  const newSegments = {};
  if (payload && !oldPayload) {
    setSegments.forEach((s) => {
      newSegments[s] = true;
    });
    oldSegments.forEach((s) => {
      newSegments[s] = true;
    });
  } else if (oldPayload) {
    setSegments.forEach((s) => {
      newSegments[s] = true;
    });
    oldSegments.forEach((s) => {
      newSegments[s] = false;
    });
  } else {
    setSegments.forEach((s) => {
      newSegments[s] = false;
    });
    oldSegments.forEach((s) => {
      newSegments[s] = false;
    });
  }

  // return null for empty segments
  if (Object.keys(newSegments).length) {
    return [id, newPayload, newSegments];
  } else {
    return [id, newPayload, null];
  }
}
