import snowflake from "snowflake-sdk";
import { Readable } from "stream";
import { log } from '../../utils/logger.ts';

export class Snowflake {
  protected connection: snowflake.Connection;

  constructor(account: string, username: string, password: string, database: string) {
    this.connection = snowflake.createConnection({
      account,
      username,
      password,
      database,
    });
  }

  connect() {
    return new Promise((resolve, reject) => this.connection.connect((err, conn) => {
      if (err) {
        reject(err);
      } else {
        resolve(conn);
      }
    }));
  }

  getStream(sqlText: string, batchSize: number, hwm: number): Readable {
    const connection = this.connection;
    let offset = 0;
    let totalCount = 0;
    let fetching = false;
    let ended = false;
    let rowBuffer: unknown[] = [];

    const executePage = (sql: string): Promise<unknown[]> =>
      new Promise((resolve, reject) =>
        connection.execute({
          sqlText: sql,
          complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows ?? [])),
        })
      );

    const readStream = new Readable({
      objectMode: true,
      highWaterMark: hwm,

      read() {
        // Step 1: drain any rows left over from a previous backpressure stop
        while (rowBuffer.length > 0) {
          const row = rowBuffer.shift();
          totalCount++;
          if (totalCount % 10000 === 0) log(`streaming... ${totalCount}`);
          if (!readStream.push(row)) return;
        }

        // Step 2: buffer empty — if last page was already fetched, end the stream
        if (ended) {
          log(`stream ended, total rows: ${totalCount}`);
          readStream.push(null);
          return;
        }

        // Step 3: guard against a concurrent async fetch still in flight
        if (fetching) return;
        fetching = true;

        const pagedSql = `${sqlText} LIMIT ${batchSize} OFFSET ${offset}`;
        log(`Fetching page at offset ${offset}`);

        executePage(pagedSql)
          .then(rows => {
            fetching = false;
            offset += rows.length;
            if (rows.length < batchSize) ended = true;

            // Push rows one by one; on backpressure save the remainder to rowBuffer
            for (let i = 0; i < rows.length; i++) {
              totalCount++;
              if (totalCount % 10000 === 0) log(`streaming... ${totalCount}`);
              if (!readStream.push(rows[i])) {
                rowBuffer = rows.slice(i + 1);
                return;
              }
            }

            // All rows from this page pushed successfully
            if (ended) {
              log(`stream ended, total rows: ${totalCount}`);
              readStream.push(null);
            }
          })
          .catch(err => {
            log(`stream error: ${err}`, err);
            readStream.destroy(err as Error);
          });
      },
    });

    log('streaming started');
    return readStream;
  }

  close() {
    return new Promise((resolve, reject) => this.connection.destroy((err, conn) => {
      if (err) {
        reject(err);
      } else {
        resolve(conn);
      }
    }));
  }
}
