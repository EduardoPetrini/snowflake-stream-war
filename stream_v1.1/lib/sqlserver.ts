import sql from "mssql";
import { Writable, Readable } from "stream";
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
      options: {
        trustServerCertificate: true,
      }
    });
  }

  connect() {
    return this.connection.connect();
  }

  getReadStream(sqlText: string) {
    const request = new sql.Request(this.connection);
    request.stream = true;
    let count = 0;

    const readStream = new Readable({
      objectMode: true,
      read() {
        request.resume();
      }
    });

    request.on('row', (data) => {
      count++;
      if (count % 10000 === 0) {
        log(`streaming... ${count}`);
      }
      if (!readStream.push(data)) {
        log(`pausing stream`);
        request.pause();
      }
    });

    request.on('error', (err) => {
      log(err);
      readStream.destroy();
    });

    request.on('done', () => {
      log('done');
      readStream.push(null);
    });

    request.query(sqlText);
    return readStream;
  }

  getWriteStream() {
    let count = 0;
    return new Writable({
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
