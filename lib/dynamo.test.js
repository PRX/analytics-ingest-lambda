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

  describe(".upsertRedirectResult", () => {
    it("returns null payload", async () => {
      const id = "id";
      const segments = ["1"];
      const result = { Attributes: { segments: { SS: ["1", "2"] } } };

      expect(await dynamo.upsertRedirectResult({ id })).toEqual([id, null, null]);
      expect(await dynamo.upsertRedirectResult({ id, segments })).toEqual([id, null, { 1: false }]);
      expect(await dynamo.upsertRedirectResult({ id, segments, result })).toEqual([
        id,
        null,
        { 1: false, 2: false },
      ]);
    });

    it("returns null when no segments", async () => {
      const id = "id";
      const payload = { foo: "bar" };
      const result = { Attributes: { payload: { B: await dynamo.deflate({ foo: "bar" }) } } };

      const expected = [id, payload, null];
      expect(await dynamo.upsertRedirectResult({ id, payload })).toEqual(expected);
      expect(await dynamo.upsertRedirectResult({ id, payload, result })).toEqual(expected);
    });

    it("returns null when segments all already set", async () => {
      const id = "id";
      const payload = { foo: "bar" };
      const result = {
        Attributes: {
          payload: { B: await dynamo.deflate({ foo: "bar" }) },
          segments: { SS: ["1", "2", "3"] },
        },
      };

      const args = { id, payload, result };
      const r1 = await dynamo.upsertRedirectResult({ ...args, segments: ["1"] });
      const r2 = await dynamo.upsertRedirectResult({ ...args, segments: ["2", "1"] });
      const r3 = await dynamo.upsertRedirectResult({ ...args, segments: ["2", "3", "1"] });

      expect(r1).toEqual(["id", { foo: "bar" }, { 1: false, 2: false, 3: false }]);
      expect(r2).toEqual(r1);
      expect(r3).toEqual(r1);
    });

    it("returns all segments when first setting payload", async () => {
      const id = "id";
      const payload = { foo: "bar" };
      const segments = ["2", "3"];
      const result = { Attributes: { segments: { SS: ["1", "2"] } } };

      const res = await dynamo.upsertRedirectResult({ id, payload, segments, result });
      expect(res[0]).toEqual(id);
      expect(res[1]).toEqual(payload);
      expect(res[2]).toEqual({ 1: true, 2: true, 3: true });
    });

    it("returns all segments when first setting segments", async () => {
      const id = "id";
      const payload = { foo: "bar" };
      const segments = ["1", "2"];
      const result = { Attributes: { payload: { B: await dynamo.deflate({ foo: "bar" }) } } };

      const res = await dynamo.upsertRedirectResult({ id, payload, segments, result });
      expect(res[0]).toEqual(id);
      expect(res[1]).toEqual(payload);
      expect(res[2]).toEqual({ 1: true, 2: true });
    });

    it("returns new segments when subsequently setting segments", async () => {
      const id = "id";
      const payload = { foo: "changed" };
      const segments = ["1", "2", "3"];
      const result = {
        Attributes: {
          payload: { B: await dynamo.deflate({ foo: "bar" }) },
          segments: { SS: ["1", "2"] },
        },
      };

      const res = await dynamo.upsertRedirectResult({ id, payload, segments, result });
      expect(res[0]).toEqual(id);
      expect(res[1]).toEqual(payload);
      expect(res[2]).toEqual({ 1: false, 2: false, 3: true });
    });

    it("merges just-set extras into the payload", async () => {
      const id = "id";
      const payload = { foo: "bar" };
      const extras = { extra: "stuff" };

      const res = await dynamo.upsertRedirectResult({ id, payload, extras });
      expect(res[1]).toEqual({ ...payload, ...extras });
    });

    it("merges previous extras into the payload", async () => {
      const id = "id";
      const payload = { foo: "bar" };
      const extras = { extra: "stuff" };
      const result = {
        Attributes: {
          payload: { B: await dynamo.deflate(payload) },
          extras: { S: JSON.stringify(extras) },
        },
      };

      const res = await dynamo.upsertRedirectResult({ id, result });
      expect(res[1]).toEqual({ ...payload, ...extras });
    });
  });
});
