import sql from "mssql";
import { Writable } from "stream";
import { setTimeout } from "timers/promises";

export class SQLServer {
  protected connection: sql.ConnectionPool;

  constructor(server: string, database: string, user: string, password: string) {
    this.connection = new sql.ConnectionPool({
      server,
      database,
      user,
      password,
    });
  }

  connect() {
    return this.connection.connect();
  }

  getWriteStream() {
    let count = 0;
    return new Writable({
      objectMode: true,
      async write(chunk, encoding, callback) {
        await setTimeout(Math.random() * 100);
        console.log(`Wrote ${++count} rows`);
        callback();
      }
    });
  }

  close() {
    return this.connection.close();
  }
}
