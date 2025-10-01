import { promisify } from "node:util";
import { deflate as zdeflate, inflate as zinflate } from "node:zlib";
import { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { AssumeRoleCommand, STSClient } from "@aws-sdk/client-sts";
import log from "lambda-log";
import DynamoData from "./dynamo-data";

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
  return new DynamoData({ id, payload, segments, extras, result });
}

/**
 * Helper to do DDB operations concurrently
 */
export async function concurrently(num, queue, workerFn) {
  const threads = Array(num).fill(true);

  let success = 0;
  let failure = 0;

  await Promise.all(
    threads.map(async () => {
      let args = queue.shift();
      while (args) {
        try {
          await workerFn(args);
          success++;
        } catch (err) {
          if (err.name === "ProvisionedThroughputExceededException") {
            log.warn(`DDB Throughput Exceeded: ${err}`, { err, args });
          } else {
            log.error(`DDB Error: ${err}`, { err, args });
          }
          failure++;
        }
        args = queue.shift();
      }
    }),
  );

  return [success, failure];
}
