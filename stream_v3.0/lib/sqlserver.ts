import sql from "mssql";
import { Writable } from "stream";
import { setTimeout } from "timers/promises";
import { log } from '../../utils/logger.ts';

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

  getWriteStream(hwm: number) {
    let count = 0;
    return new Writable({
      highWaterMark: hwm,
      objectMode: true,
      async write(chunk, encoding, callback) {
        if (!chunk?.length) {
          // log('Not writing empty chunk');
          callback();
          return;
        }
        const wait = Math.round(Math.random() * 100);
        await setTimeout(wait);
        count += chunk.length;
        log(`Wrote ${count} rows, wait: ${wait}`);
        callback();
      }
    });
  }

  close() {
    return this.connection.close();
  }
}
