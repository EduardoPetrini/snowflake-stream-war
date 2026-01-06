import { Transform, } from "node:stream";
import { log } from '../../utils/logger.ts';

export class Batch {
  protected batchSize: number;

  constructor(batchSize: number) {
    this.batchSize = batchSize;
  }

  getTransformStream(hwm: number) {
    let buffer: any[] = [];
    const batchSize = this.batchSize;
    return new Transform({
      highWaterMark: hwm,
      objectMode: true,
      transform(this: Transform, chunk: any, encoding: any, callback: any) {
        buffer.push(chunk);
        // log(`Transforming chunk, buffer size: ${buffer.length}`);
        if (buffer.length >= batchSize) {
          const pushed = this.push(buffer);
          buffer = [];
        }
        callback();
      },
      flush(this: Transform, callback: any) {
        log(`Flushing buffer, buffer size: ${buffer.length}`);
        if (buffer.length > 0) {
          this.push(buffer);
          buffer = [];
        }

        callback();
      }
    });
  }
} 
