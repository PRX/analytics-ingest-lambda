import { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { AssumeRoleCommand, STSClient } from "@aws-sdk/client-sts";
import { jest } from "@jest/globals";
import "aws-sdk-client-mock-jest";
import { mockClient } from "aws-sdk-client-mock";
import log from "lambda-log";
import * as dynamo from "./dynamo";

describe("dynamo", () => {
  describe(".client", () => {
    it("returns a ddb client", async () => {
      const client = await dynamo.client();
      expect(client).toBeInstanceOf(DynamoDBClient);
    });

    it("assumes an sts role then returns a ddb client", async () => {
      const stsMock = mockClient(STSClient);
      const Credentials = { AccessKeyId: "a", SecretAccessKey: "b", SessionToken: "c" };
      stsMock.on(AssumeRoleCommand).resolves({ Credentials });

      const client = await dynamo.client("arn:aws:iam::1234:role/some-role");
      expect(client).toBeInstanceOf(DynamoDBClient);
      const creds = await client.config.credentials();
      expect(creds.accessKeyId).toEqual("a");
      expect(creds.secretAccessKey).toEqual("b");
      expect(creds.sessionToken).toEqual("c");
    });

    it("returns a default client on sts error", async () => {
      const stsMock = mockClient(STSClient);
      stsMock.on(AssumeRoleCommand).rejects(new Error("bad stuff"));
      jest.spyOn(log, "error").mockReturnValue();

      const client = await dynamo.client("arn:aws:iam::1234:role/some-role");
      expect(client).toBeInstanceOf(DynamoDBClient);
      expect(log.error.mock.calls.length).toEqual(1);
      expect(log.error.mock.calls[0][0]).toMatch(/dynamodb sts error/i);
      expect(log.error.mock.calls[0][1].error.message).toMatch(/bad stuff/);
    });
  });

  describe(".upsertRedirect", () => {
    it("sets params", async () => {
      const ddbMock = mockClient(DynamoDBClient);
      ddbMock.on(UpdateItemCommand).resolves({});

      await dynamo.upsertRedirect({ id: "my-id" });

      expect(ddbMock).toHaveReceivedCommandWith(UpdateItemCommand, {
        Key: { id: { S: "my-id" } },
        ReturnValues: "ALL_OLD",
        TableName: process.env.DDB_TABLE,
      });
    });

    it("deflates payloads", async () => {
      const ddbMock = mockClient(DynamoDBClient);
      ddbMock.on(UpdateItemCommand).resolves({});

      await dynamo.upsertRedirect({ id: "my-id", payload: { some: "payload" } });

      const payload = await dynamo.deflate({ some: "payload" });
      expect(ddbMock).toHaveReceivedCommandWith(UpdateItemCommand, {
        AttributeUpdates: {
          payload: { Action: "PUT", Value: { B: payload } },
        },
      });
    });

    it("stringifies segments", async () => {
      const ddbMock = mockClient(DynamoDBClient);
      ddbMock.on(UpdateItemCommand).resolves({});

      await dynamo.upsertRedirect({ id: "my-id", segments: [1, "2", "three"] });

      expect(ddbMock).toHaveReceivedCommandWith(UpdateItemCommand, {
        AttributeUpdates: {
          segments: { Action: "ADD", Value: { SS: ["1", "2", "three"] } },
        },
      });
    });

    it("optionally adds an expiration", async () => {
      const now = Math.round(Date.now() / 1000);
      const ddbMock = mockClient(DynamoDBClient);
      ddbMock.on(UpdateItemCommand).resolves({});
      process.env.DDB_TTL = 100;

      await dynamo.upsertRedirect({ id: "my-id" });

      expect(ddbMock).toHaveReceivedCommandWith(UpdateItemCommand, {
        AttributeUpdates: {
          expiration: { Action: "PUT", Value: { N: expect.anything() } },
        },
      });

      const exp = ddbMock.call(0).args[0].input.AttributeUpdates.expiration.Value.N;
      expect(parseInt(exp, 10)).toBeGreaterThanOrEqual(now + 100);
      expect(parseInt(exp, 10)).toBeLessThanOrEqual(now + 101);
    });
  });
});
