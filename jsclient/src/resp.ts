// RESP protocol encoder/decoder for Valkey.

const CRLF = "\r\n";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Encode a command (array of strings) into a RESP request buffer. */
export function encode(args: string[]): Uint8Array {
  let s = `*${args.length}${CRLF}`;
  for (const a of args) {
    const bytes = encoder.encode(a);
    s += `$${bytes.byteLength}${CRLF}${a}${CRLF}`;
  }
  return encoder.encode(s);
}

// ─── Response types ──────────────────────────────────────────────────────────

export type RespValue =
  | { type: "string"; value: string }
  | { type: "error"; value: string }
  | { type: "integer"; value: number }
  | { type: "bulk"; value: string | null }
  | { type: "array"; value: RespValue[] | null };

/** Stateful RESP parser that consumes bytes incrementally. */
export class RespReader {
  private buf = Buffer.alloc(0);
  private pos = 0;

  /** Append new data received from the socket. */
  append(data: Uint8Array): void {
    if (this.pos > 0) {
      this.buf = Buffer.from(this.buf.subarray(this.pos));
      this.pos = 0;
    }
    this.buf = Buffer.concat([this.buf, data]);
  }

  /**
   * Try to parse one complete RESP value from the buffer.
   * Returns null if more data is needed.
   */
  tryRead(): RespValue | null {
    const saved = this.pos;
    const result = this.readValue();
    if (result === null) {
      this.pos = saved; // rollback
    }
    return result;
  }

  private readValue(): RespValue | null {
    if (this.pos >= this.buf.length) return null;
    const prefix = this.buf[this.pos++];

    switch (prefix) {
      case 0x2b: // '+'
        return this.readSimpleString();
      case 0x2d: // '-'
        return this.readError();
      case 0x3a: // ':'
        return this.readInteger();
      case 0x24: // '$'
        return this.readBulkString();
      case 0x2a: // '*'
        return this.readArray();
      default:
        throw new Error(`Unknown RESP prefix: ${String.fromCharCode(prefix)}`);
    }
  }

  private readLine(): string | null {
    const idx = this.buf.indexOf("\r\n", this.pos);
    if (idx === -1) return null;
    const line = decoder.decode(this.buf.subarray(this.pos, idx));
    this.pos = idx + 2;
    return line;
  }

  private readSimpleString(): RespValue | null {
    const line = this.readLine();
    if (line === null) return null;
    return { type: "string", value: line };
  }

  private readError(): RespValue | null {
    const line = this.readLine();
    if (line === null) return null;
    return { type: "error", value: line };
  }

  private readInteger(): RespValue | null {
    const line = this.readLine();
    if (line === null) return null;
    return { type: "integer", value: parseInt(line, 10) };
  }

  private readBulkString(): RespValue | null {
    const line = this.readLine();
    if (line === null) return null;
    const len = parseInt(line, 10);
    if (len < 0) return { type: "bulk", value: null };
    if (this.pos + len + 2 > this.buf.length) return null; // need more data
    const data = decoder.decode(this.buf.subarray(this.pos, this.pos + len));
    this.pos += len + 2; // skip data + \r\n
    return { type: "bulk", value: data };
  }

  private readArray(): RespValue | null {
    const line = this.readLine();
    if (line === null) return null;
    const count = parseInt(line, 10);
    if (count < 0) return { type: "array", value: null };
    const elems: RespValue[] = [];
    for (let i = 0; i < count; i++) {
      const el = this.readValue();
      if (el === null) return null;
      elems.push(el);
    }
    return { type: "array", value: elems };
  }
}
