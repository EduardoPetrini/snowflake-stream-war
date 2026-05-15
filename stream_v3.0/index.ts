import { Batch, Snowflake, SQLServer } from "./lib/index.ts";
import { pipeline } from "node:stream/promises";
import 'js-monitor-server';

const account = process.env.SF_ACCOUNT || "";
const username = process.env.SF_USERNAME || "";
const password = process.env.SF_PASSWORD || "";
const database = process.env.SF_DATABASE || "";

const sqlText = process.env.SF_QUERY || "";

const batchSize = Number(process.env.BATCH_SIZE) || 100;

const snowflake = new Snowflake(account, username, password, database);
const sqlserver = new SQLServer("", "", "", "");
const batch = new Batch(batchSize);

const readHwm = Number(process.env.V2_READ_HWM) || 1000;
const transformHwm = Number(process.env.V2_TRANSFORM_HWM) || 1;
const writeHwm = Number(process.env.V2_WRITE_HWM) || 1;

try {
  await snowflake.connect();
  // await sqlserver.connect();

  const readStream = await snowflake.getStream(sqlText, readHwm);
  const transformStream = batch.getTransformStream(transformHwm);
  const writeStream = sqlserver.getWriteStream(writeHwm);

  await pipeline(readStream, transformStream, writeStream);
  console.log("Done");
} catch (err) {
  console.error("Pipeline failed:", err);
  process.exit(1);
}
