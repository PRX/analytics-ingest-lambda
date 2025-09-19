import { BigQuery } from "@google-cloud/bigquery";
import log from "lambda-log";

/**
 * Create a bigquery client
 */
export function client(credentials = process.env.BQ_CREDENTIALS) {
  if (credentials) {
    try {
      credentials = JSON.parse(credentials);
    } catch (_err) {
      throw new Error(`Invalid BQ Credentials: ${credentials}`);
    }
  } else {
    throw new Error("Missing BQ Credentials");
  }
  return new BigQuery({ credentials });
}

/**
 * Streaming inserts
 */
const getClient = client;
export async function insert({ client, dataset, table, rows, retries = 2 } = {}) {
  client = client || getClient();
  dataset = dataset || process.env.BQ_DATASET;
  if (!rows || rows.length === 0) {
    return 0;
  }
  if (!table) {
    throw new Error("Missing BQ Table");
  }

  try {
    await client.dataset(dataset).table(table).insert(rows, { raw: true });
    return rows.length;
  } catch (err) {
    if (err.name === "PartialFailureError") {
      throw logPartialFailureErrors(err);
    } else if (err.message.match(/client_email|private|key|pem_read|invalid_client|project/i)) {
      throw new Error("Invalid BQ Credentials");
    } else if (err.message.match(/not found: table/i)) {
      throw new Error(`Invalid BQ Table: ${table}`);
    } else if (retries > 0) {
      log.warn("BQ Insert Retry", { error: err });
      return insert({ client, dataset, table, rows, retries: retries - 1 });
    } else {
      throw err;
    }
  }
}

/**
 * Log out errors for any PartialFailureError
 * NOTE: these are retried internally by the SDK
 */
export function logPartialFailureErrors(err) {
  const count = err.errors?.length || 1;
  for (const e of err.errors) {
    log.error("PartialFailureError", { errors: e.errors || e, row: e.row });
  }
  throw new Error(`BQ Insert PartialFailureErrors: ${count}`);
}
