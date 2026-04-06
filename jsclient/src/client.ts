import { encode, RespReader, type RespValue } from "./resp";

export class ValkeyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ValkeyError";
  }
}

export const ErrNil = new ValkeyError("valkey: nil");

interface Pending {
  resolve: (v: RespValue) => void;
  reject: (e: Error) => void;
}

export class Client {
  private socket: ReturnType<typeof Bun.connect> extends Promise<infer T>
    ? T
    : never;
  private reader = new RespReader();
  private queue: Pending[] = [];
  private connected = false;

  private constructor() {
    // use Dial()
    this.socket = null!;
  }

  /** Connect to a Valkey server. addr is "host:port" (e.g. "127.0.0.1:6379"). */
  static async dial(addr: string): Promise<Client> {
    const [host, portStr] = addr.includes(":")
      ? [
          addr.slice(0, addr.lastIndexOf(":")),
          addr.slice(addr.lastIndexOf(":") + 1),
        ]
      : ["127.0.0.1", addr];
    const port = parseInt(portStr, 10);
    const hostname = host === "" ? "127.0.0.1" : host;

    const client = new Client();

    client.socket = await Bun.connect({
      hostname,
      port,
      socket: {
        data(_socket, data) {
          client.reader.append(new Uint8Array(data));
          client.drain();
        },
        error(_socket, err) {
          // reject all pending
          for (const p of client.queue.splice(0)) {
            p.reject(err);
          }
        },
        close() {
          client.connected = false;
          for (const p of client.queue.splice(0)) {
            p.reject(new Error("connection closed"));
          }
        },
        open() {
          client.connected = true;
        },
      },
    });

    return client;
  }

  /** Send a raw RESP command and return the parsed response. */
  async do(...args: string[]): Promise<RespValue> {
    if (!this.connected) throw new Error("not connected");
    const buf = encode(args);
    this.socket.write(buf);

    return new Promise<RespValue>((resolve, reject) => {
      this.queue.push({ resolve, reject });
    });
  }

  /** Close the connection. */
  close(): void {
    this.socket.end();
    this.connected = false;
  }

  private drain(): void {
    while (this.queue.length > 0) {
      const val = this.reader.tryRead();
      if (val === null) break;
      const pending = this.queue.shift()!;
      pending.resolve(val);
    }
  }

  // ─── String commands ─────────────────────────────────────────────────────

  async set(key: string, value: string): Promise<void> {
    const v = await this.do("SET", key, value);
    throwIfError(v);
  }

  async setEX(key: string, value: string, seconds: number): Promise<void> {
    const v = await this.do("SET", key, value, "EX", String(seconds));
    throwIfError(v);
  }

  async setPX(
    key: string,
    value: string,
    milliseconds: number,
  ): Promise<void> {
    const v = await this.do("SET", key, value, "PX", String(milliseconds));
    throwIfError(v);
  }

  async get(key: string): Promise<string | null> {
    const v = await this.do("GET", key);
    throwIfError(v);
    if (v.type === "bulk" && v.value === null) return null;
    if (v.type === "bulk") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async del(...keys: string[]): Promise<number> {
    const v = await this.do("DEL", ...keys);
    throwIfError(v);
    if (v.type === "integer") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  // ─── TTL / expiry ────────────────────────────────────────────────────────

  async expire(key: string, seconds: number): Promise<boolean> {
    const v = await this.do("EXPIRE", key, String(seconds));
    throwIfError(v);
    return v.type === "integer" && v.value === 1;
  }

  async pExpire(key: string, milliseconds: number): Promise<boolean> {
    const v = await this.do("PEXPIRE", key, String(milliseconds));
    throwIfError(v);
    return v.type === "integer" && v.value === 1;
  }

  async ttl(key: string): Promise<number> {
    const v = await this.do("TTL", key);
    throwIfError(v);
    if (v.type === "integer") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async pTTL(key: string): Promise<number> {
    const v = await this.do("PTTL", key);
    throwIfError(v);
    if (v.type === "integer") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async persist(key: string): Promise<boolean> {
    const v = await this.do("PERSIST", key);
    throwIfError(v);
    return v.type === "integer" && v.value === 1;
  }

  // ─── Hash commands ───────────────────────────────────────────────────────

  async hSet(key: string, ...fieldValues: string[]): Promise<number> {
    if (fieldValues.length % 2 !== 0) {
      throw new ValkeyError("hSet requires even number of field/value args");
    }
    const v = await this.do("HSET", key, ...fieldValues);
    throwIfError(v);
    if (v.type === "integer") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async hGet(key: string, field: string): Promise<string | null> {
    const v = await this.do("HGET", key, field);
    throwIfError(v);
    if (v.type === "bulk" && v.value === null) return null;
    if (v.type === "bulk") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async hDel(key: string, ...fields: string[]): Promise<number> {
    const v = await this.do("HDEL", key, ...fields);
    throwIfError(v);
    if (v.type === "integer") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async hGetAll(key: string): Promise<Record<string, string>> {
    const v = await this.do("HGETALL", key);
    throwIfError(v);
    if (v.type !== "array" || v.value === null) return {};
    const result: Record<string, string> = {};
    for (let i = 0; i + 1 < v.value.length; i += 2) {
      const k = v.value[i];
      const val = v.value[i + 1];
      if (k.type === "bulk" && k.value !== null && val.type === "bulk") {
        result[k.value] = val.value ?? "";
      }
    }
    return result;
  }

  async hLen(key: string): Promise<number> {
    const v = await this.do("HLEN", key);
    throwIfError(v);
    if (v.type === "integer") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async hExists(key: string, field: string): Promise<boolean> {
    const v = await this.do("HEXISTS", key, field);
    throwIfError(v);
    return v.type === "integer" && v.value === 1;
  }

  async hKeys(key: string): Promise<string[]> {
    const v = await this.do("HKEYS", key);
    throwIfError(v);
    return bulkArray(v);
  }

  async hVals(key: string): Promise<string[]> {
    const v = await this.do("HVALS", key);
    throwIfError(v);
    return bulkArray(v);
  }

  // ─── JSON commands ───────────────────────────────────────────────────────

  async jsonSet(key: string, path: string, value: unknown): Promise<void> {
    const data = JSON.stringify(value);
    const v = await this.do("JSON.SET", key, path, data);
    throwIfError(v);
  }

  async jsonGet(key: string, path = "$"): Promise<string | null> {
    const v = await this.do("JSON.GET", key, path);
    throwIfError(v);
    if (v.type === "bulk" && v.value === null) return null;
    if (v.type === "bulk") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async jsonDel(key: string, path = "$"): Promise<number> {
    const v = await this.do("JSON.DEL", key, path);
    throwIfError(v);
    if (v.type === "integer") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async jsonType(key: string, path = "$"): Promise<string | null> {
    const v = await this.do("JSON.TYPE", key, path);
    throwIfError(v);
    if (v.type === "string") return v.value;
    if (v.type === "bulk" && v.value === null) return null;
    if (v.type === "bulk") return v.value;
    throw new ValkeyError("unexpected response type");
  }

  async jsonNumIncrBy(
    key: string,
    path: string,
    n: number,
  ): Promise<number> {
    const v = await this.do("JSON.NUMINCRBY", key, path, String(n));
    throwIfError(v);
    if (v.type === "bulk" && v.value !== null) return parseFloat(v.value);
    throw new ValkeyError("unexpected response type");
  }

  // ─── Server ──────────────────────────────────────────────────────────────

  async ping(): Promise<string> {
    const v = await this.do("PING");
    throwIfError(v);
    if (v.type === "string") return v.value;
    throw new ValkeyError("unexpected response type");
  }
}

// ─── helpers ───────────────────────────────────────────────────────────────

function throwIfError(v: RespValue): void {
  if (v.type === "error") throw new ValkeyError(v.value);
}

function bulkArray(v: RespValue): string[] {
  if (v.type !== "array" || v.value === null) return [];
  return v.value
    .filter((e): e is { type: "bulk"; value: string } => e.type === "bulk" && e.value !== null)
    .map((e) => e.value);
}
