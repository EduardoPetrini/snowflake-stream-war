import { Batch, Snowflake, SQLServer } from "./lib/index.ts";
import { pipeline } from "node:stream/promises";
import 'js-monitor-server';

const account = process.env.SF_ACCOUNT || "";
const username = process.env.SF_USERNAME || "";
const password = process.env.SF_PASSWORD || "";
const database = process.env.SF_DATABASE || "";

const sfSqlText = process.env.SF_QUERY || "";
const msSqlText = process.env.MS_QUERY || "";

const batchSize = Number(process.env.BATCH_SIZE) || 100;

const msHostname = process.env.MS_HOSTNAME || "";
const msUsername = process.env.MS_USERNAME || "";
const msPassword = process.env.MS_PASSWORD || "";
const msDatabase = process.env.MS_DATABASE || "";

const snowflake = new Snowflake(account, username, password, database);
const sqlserver = new SQLServer(msHostname, msDatabase, msUsername, msPassword);
const batch = new Batch(batchSize);

try {
  // await snowflake.connect();
  await sqlserver.connect();

  // const readStream = await snowflake.getStream(sqlText);
  const transformStream = batch.getTransformStream();
  const readStream = sqlserver.getReadStream(msSqlText);
  const writeStream = sqlserver.getWriteStream();

  await pipeline(readStream, transformStream, writeStream);
  console.log("Done");
} catch (err) {
  console.error("Pipeline failed:", err);
  process.exit(1);
}
