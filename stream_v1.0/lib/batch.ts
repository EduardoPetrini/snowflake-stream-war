import { Transform, } from "node:stream";
import { log } from '../../utils/logger.ts';

export class Batch {
  protected batchSize: number;

  constructor(batchSize: number) {
    this.batchSize = batchSize;
  }

  getTransformStream() {
    const buffer: any[] = [];
    const batchSize = this.batchSize;
    return new Transform({
      objectMode: true,
      transform(this: Transform, chunk: any, encoding) {
        buffer.push(chunk);
        log(`Transforming chunk, buffer size: ${buffer.length}`);
        if (buffer.length >= batchSize) {
          this.push(buffer);
          buffer.length = 0;
        }
      },

      flush(this: Transform) {
        log(`Flushing buffer, buffer size: ${buffer.length}`);
        if (buffer.length > 0) {
          this.push(buffer);
          buffer.length = 0;
        }
      }
    });
  }
} 
