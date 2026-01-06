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

  async getStream(sqlText: string): Promise<Readable> {
    const statement: snowflake.RowStatement = await new Promise((resolve, reject) => this.connection.execute({
      sqlText,
      streamResult: true,
      complete: (err, stmt) => {
        if (err) {
          reject(err);
        } else {
          resolve(stmt);
        }
      }
    }));

    const stream: Readable = statement.streamRows();
    let count = 0;
    let started = false;
    const readStream = new Readable({
      objectMode: true,
      read() {
        if (started) {
          return;
        }

        log("streaming started");
        started = true;

        stream.on("data", (data) => {
          count++;
          if (count % 10000 === 0) {
            log(`streaming... ${count}`);
          }
          this.push(data);
        });
        stream.on("end", () => {
          log(`stream ended, total rows: ${count}`);
          this.push(null);
        });
      }
    });

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
