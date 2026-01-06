import { Batch, Snowflake, SQLServer } from "./lib/index.ts";
import { pipeline } from "node:stream/promises";

const account = process.env.SF_ACCOUNT || "";
const username = process.env.SF_USERNAME || "";
const password = process.env.SF_PASSWORD || "";
const database = process.env.SF_DATABASE || "";

const sqlText = process.env.SF_QUERY || "";

const snowflake = new Snowflake(account, username, password, database);
const sqlserver = new SQLServer("", "", "", "");
const batch = new Batch(100);

try {
  await snowflake.connect();
  // await sqlserver.connect();

  const readStream = await snowflake.getStream(sqlText);
  const transformStream = batch.getTransformStream();
  const writeStream = sqlserver.getWriteStream();

  await pipeline(readStream, transformStream, writeStream);
  console.log("Done");
} catch (err) {
  console.error("Pipeline failed:", err);
  process.exit(1);
}
