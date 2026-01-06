import { Transform, } from "node:stream";

export class Batch {
  protected batchSize: number;

  constructor(batchSize: number) {
    this.batchSize = batchSize;
  }

  getTransformStream() {
    const buffer: any[] = [];
    const batchSize = this.batchSize;
    return new Transform({
      transform(this: Transform, chunk: any, encoding) {
        buffer.push(chunk);
        if (buffer.length >= batchSize) {
          this.push(buffer);
          buffer.length = 0;
        }
      },

      flush(this: Transform) {
        if (buffer.length > 0) {
          this.push(buffer);
          buffer.length = 0;
        }
      }
    });
  }
} 
