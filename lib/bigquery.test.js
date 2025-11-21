import { BigQuery } from "@google-cloud/bigquery";
import { jest } from "@jest/globals";
import log from "lambda-log";
import * as bigquery from "./bigquery";

describe("bigquery", () => {
  describe(".client", () => {
    it("returns a bigquery client", () => {
      const client = bigquery.client("{}");
      expect(client).toBeInstanceOf(BigQuery);
    });

    it("requires credentials", () => {
      expect(() => bigquery.client()).toThrow(/missing bq credentials/i);
    });

    it("handles json parse errors", () => {
      expect(() => bigquery.client("abcd")).toThrow(/invalid bq credentials/i);
    });
  });

  describe(".insert", () => {
    const mockClient = (insert = null) => ({ dataset: () => ({ table: () => ({ insert }) }) });

    it("inserts rows", async () => {
      const insert = jest.fn().mockResolvedValue(null);
      const client = mockClient(insert);
      const rows = [{ row: "one" }, { row: "two" }];
      const table = "some_table";

      expect(await bigquery.insert({ client, rows, table })).toEqual(2);
      expect(insert).toHaveBeenCalledTimes(1);
      expect(insert).toHaveBeenCalledWith(rows, { raw: true });
    });

    it("inserts nothing", async () => {
      expect(await bigquery.insert({ client: mockClient(), rows: null })).toEqual(0);
      expect(await bigquery.insert({ client: mockClient(), rows: [] })).toEqual(0);
    });

    it("requires a table", async () => {
      const client = mockClient();
      const rows = [{ row: "one" }];
      const table = "";
      await expect(bigquery.insert({ client, rows, table })).rejects.toThrow(/missing bq table/i);
    });

    it("throws partial insert errors", async () => {
      jest.spyOn(log, "error").mockReturnValue();

      const err = {
        name: "PartialFailureError",
        errors: [
          { row: "row1", errors: ["the", "errors"] },
          { row: "row2", errors: ["more", "errors"] },
          { unexpected: "something" },
        ],
      };
      const insert = jest.fn().mockRejectedValue(err);
      const client = mockClient(insert);
      const args = { client, rows: [{}], table: "t" };

      await expect(bigquery.insert(args)).rejects.toThrow(/bq insert partialfailureerrors: 3/i);
      expect(insert).toHaveBeenCalledTimes(1);
    });

    it("throws credential errors", async () => {
      const err = new Error("Something about Client_email is bad or something");
      const insert = jest.fn().mockRejectedValue(err);
      const client = mockClient(insert);
      const args = { client, rows: [{}], table: "t" };

      await expect(bigquery.insert(args)).rejects.toThrow(/invalid bq credentials/i);
      expect(insert).toHaveBeenCalledTimes(1);
    });

    it("throws table not found errors", async () => {
      const err = new Error("Something something Not found: Table t does not exist");
      const insert = jest.fn().mockRejectedValue(err);
      const client = mockClient(insert);
      const args = { client, rows: [{}], table: "t" };

      await expect(bigquery.insert(args)).rejects.toThrow(/invalid bq table/i);
      expect(insert).toHaveBeenCalledTimes(1);
    });

    it("retries other errors", async () => {
      jest.spyOn(log, "warn").mockReturnValue();

      const err = new Error("bad stuff");
      const insert = jest.fn().mockRejectedValue(err);
      const client = mockClient(insert);
      const args = { client, rows: [{}], table: "t" };

      await expect(bigquery.insert(args)).rejects.toThrow(/bad stuff/i);
      expect(insert).toHaveBeenCalledTimes(3);
    });
  });

  describe(".logPartialFailureErrors", () => {
    it("logs partial failure errors", () => {
      jest.spyOn(log, "error").mockReturnValue();

      const err = {
        name: "PartialFailureError",
        errors: [
          { row: "row1", errors: ["the", "errors"] },
          { row: "row2", errors: ["more", "errors"] },
        ],
      };
      expect(() => bigquery.logPartialFailureErrors(err)).toThrow(/partialfailureerrors/i);

      expect(log.error.mock.calls.length).toEqual(2);
      expect(log.error.mock.calls[0][0]).toEqual("PartialFailureError");
      expect(log.error.mock.calls[0][1]).toEqual(err.errors[0]);
      expect(log.error.mock.calls[1][0]).toEqual("PartialFailureError");
      expect(log.error.mock.calls[1][1]).toEqual(err.errors[1]);
    });

    it("handles unexpected input", () => {
      jest.spyOn(log, "error").mockReturnValue();

      const err = {
        name: "PartialFailureError",
        errors: [{ un: "expected" }],
      };
      expect(() => bigquery.logPartialFailureErrors(err)).toThrow(/partialfailureerrors/i);

      expect(log.error.mock.calls.length).toEqual(1);
      expect(log.error.mock.calls[0][0]).toEqual("PartialFailureError");
      expect(log.error.mock.calls[0][1]).toEqual({ errors: { un: "expected" } });
    });
  });
});
