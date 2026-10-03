import { appendFileSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { RawRecord } from './envelope.ts';

/**
 * Where raw events go: FileSink for tests and no-infrastructure runs, KafkaSink
 * (kafka-sink.ts) for the real pipeline.
 *
 * Contract: `write` persists the records and *then* the checkpoint, so a crash
 * between the two can only cause a replay (handled by dedupe), never a gap.
 */
export interface RawSink {
  write(records: readonly RawRecord[], lastEventId: string): Promise<void>;
  /** Last SSE event id durably written, to continue from after a restart. */
  checkpoint(): Promise<string | undefined>;
  /** Ids of the most recently written records, to seed dedupe after a restart. */
  recentIds(limit: number): Promise<string[]>;
  close(): Promise<void>;
}

interface Checkpoint {
  lastEventId: string;
  written: number;
}

/** Append-only JSONL: one raw event per line, plus an atomically replaced checkpoint file. */
export class FileSink implements RawSink {
  private readonly dataPath: string;
  private readonly checkpointPath: string;
  private written = 0;

  constructor(dir: string) {
    mkdirSync(dir, { recursive: true });
    this.dataPath = join(dir, 'raw.jsonl');
    this.checkpointPath = join(dir, 'checkpoint.json');
    const cp = this.readCheckpoint();
    if (cp) this.written = cp.written;
  }

  async write(records: readonly RawRecord[], lastEventId: string): Promise<void> {
    if (records.length > 0) appendFileSync(this.dataPath, records.map((r) => r.data + '\n').join(''));
    this.written += records.length;
    const tmp = this.checkpointPath + '.tmp';
    writeFileSync(tmp, JSON.stringify({ lastEventId, written: this.written } satisfies Checkpoint));
    renameSync(tmp, this.checkpointPath);
  }

  async checkpoint(): Promise<string | undefined> {
    return this.readCheckpoint()?.lastEventId;
  }

  async recentIds(limit: number): Promise<string[]> {
    if (!existsSync(this.dataPath)) return [];
    const lines = readFileSync(this.dataPath, 'utf8').split('\n').filter(Boolean);
    return lines.slice(-limit).map((l) => (JSON.parse(l) as { meta: { id: string } }).meta.id);
  }

  async close(): Promise<void> {}

  private readCheckpoint(): Checkpoint | undefined {
    if (!existsSync(this.checkpointPath)) return undefined;
    return JSON.parse(readFileSync(this.checkpointPath, 'utf8')) as Checkpoint;
  }
}

/** In-memory sink for tests. */
export class MemorySink implements RawSink {
  records: RawRecord[] = [];
  last: string | undefined;

  async write(records: readonly RawRecord[], lastEventId: string): Promise<void> {
    this.records.push(...records);
    this.last = lastEventId;
  }
  async checkpoint() {
    return this.last;
  }
  async recentIds(limit: number) {
    return this.records.slice(-limit).map((r) => r.id);
  }
  async close() {}
}
