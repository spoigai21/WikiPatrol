import { streamEvents, type StreamOptions } from '../stream/sse.ts';
import { Envelope, RecentIds, type RawRecord } from './envelope.ts';
import type { RawSink } from './sink.ts';

export interface IngestStats {
  received: number;
  written: number;
  duplicates: number;
  invalid: number;
  connects: number;
}

export interface IngestOptions {
  url: string;
  sink: RawSink;
  signal?: AbortSignal;
  /** Flush when this many records are buffered... */
  batchSize?: number;
  /** ...or when the oldest buffered record is this old. */
  flushMs?: number;
  dedupeWindow?: number;
  /** Called after each batch is durably written (health probes track progress with it). */
  onWritten?: (count: number) => void;
  /** Invalid payloads go here instead of the sink; Phase 2 makes this the dead-letter topic. */
  onInvalid?: (data: string, reason: string) => void;
  log?: (msg: string) => void;
  fetchImpl?: typeof fetch;
  backoffMs?: (attempt: number) => number;
}

/**
 * SSE -> sink, raw. Restarts from the sink's checkpoint, so a dropped connection
 * or a restarted process continues where it stopped. Overlap from the reconnect is
 * removed by meta.id; nothing else about the payload is interpreted.
 */
export async function runIngester(opts: IngestOptions): Promise<IngestStats> {
  const { sink } = opts;
  const batchSize = opts.batchSize ?? 200;
  const flushMs = opts.flushMs ?? 1000;
  const log = opts.log ?? (() => {});
  const stats: IngestStats = { received: 0, written: 0, duplicates: 0, invalid: 0, connects: 0 };

  const seen = new RecentIds(opts.dedupeWindow ?? 100_000);
  for (const id of await sink.recentIds(opts.dedupeWindow ?? 100_000)) seen.add(id);

  let buffer: RawRecord[] = [];
  let bufferStarted = 0;
  let lastEventId = await sink.checkpoint();
  let flushedEventId = lastEventId;

  const flush = async () => {
    if (lastEventId === undefined || (buffer.length === 0 && lastEventId === flushedEventId)) return;
    const batch = buffer;
    buffer = [];
    await sink.write(batch, lastEventId);
    stats.written += batch.length;
    if (batch.length) opts.onWritten?.(batch.length);
    flushedEventId = lastEventId;
  };

  const streamOpts: StreamOptions = {
    onConnect: (failures, last) => {
      stats.connects++;
      log(`connect #${stats.connects}${last ? ' (resuming)' : ''}${failures ? `, after ${failures} failures` : ''}`);
    },
    onDisconnect: (reason) => log(`disconnect: ${reason}`),
  };
  if (lastEventId !== undefined) streamOpts.lastEventId = lastEventId;
  if (opts.signal) streamOpts.signal = opts.signal;
  if (opts.fetchImpl) streamOpts.fetchImpl = opts.fetchImpl;
  if (opts.backoffMs) streamOpts.backoffMs = opts.backoffMs;

  // A quiet stream still has to flush on time.
  const timer = setInterval(() => {
    if (buffer.length > 0 && Date.now() - bufferStarted >= flushMs) void flush();
  }, Math.max(50, Math.floor(flushMs / 2)));

  try {
    for await (const ev of streamEvents(opts.url, streamOpts)) {
      stats.received++;
      if (ev.id !== undefined) lastEventId = ev.id;

      let id: string;
      try {
        const env = Envelope.safeParse(JSON.parse(ev.data));
        if (!env.success) throw new Error(env.error.issues[0]?.message ?? 'invalid envelope');
        id = env.data.meta.id;
      } catch (err) {
        stats.invalid++;
        opts.onInvalid?.(ev.data, err instanceof Error ? err.message : String(err));
        continue;
      }
      if (!seen.add(id)) {
        stats.duplicates++;
        continue;
      }
      if (buffer.length === 0) bufferStarted = Date.now();
      const record: RawRecord = { id, data: ev.data };
      if (ev.id !== undefined) record.eventId = ev.id;
      buffer.push(record);
      if (buffer.length >= batchSize) await flush();
    }
  } finally {
    clearInterval(timer);
    await flush();
  }
  return stats;
}
