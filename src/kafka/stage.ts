// A pipeline stage: one input topic -> a per-message function -> one output topic (D13).
//
// Every output message carries the offset of the input message it came from. Two ways to run:
//  - live (consumer group): resumes from the group's committed offset, which is committed only
//    after the batch's outputs are written; anything re-read after a crash in between is
//    recognised by its source offset (<= the last one in the output topic) and not written again.
//  - replay (fromOffset, no group): reads an exact offset range into any output topic. For a
//    deterministic stage, the same range always gives byte-identical output (Phase 2).

import { randomUUID } from 'node:crypto';
import { Kafka, logLevel, type Admin } from 'kafkajs';
import { chunkBySize, ensureKeptTopic, readLastPerPartition } from './topics.ts';

export const SOURCE_OFFSET_HEADER = 'source-offset';

export interface StageOutput {
  key: string;
  value: string;
}

export interface Stage {
  readonly name: string;
  /** Called with each batch, in order; may be async (enrichment looks things up). */
  process(messages: readonly { value: string; offset: string }[]): Promise<{ offset: string; out: StageOutput[] }[]> | { offset: string; out: StageOutput[] }[];
}

export interface StageRunOptions {
  brokers: string[];
  input: string;
  output: string;
  stage: Stage;
  /** Replay mode: start here instead of the group's committed offset. */
  fromOffset?: string;
  /** Replay mode: stop after this offset (inclusive). */
  toOffset?: string;
  /** Live mode: the consumer group (default wikipatrol-stage-<name>). */
  groupId?: string;
  /**
   * Partitions of the output topic (default 1). Above 1, outputs are spread by key, and the
   * restart check becomes at-least-once: see `lastWrittenSource`.
   */
  outputPartitions?: number;
  signal?: AbortSignal;
  log?: (msg: string) => void;
  onBatch?: (written: number, lastOffset: string) => void;
}

export interface StageRunStats {
  read: number;
  written: number;
  skippedAlreadyWritten: number;
}

/**
 * The input offset up to which outputs are known to be written. One partition: the source offset
 * of its last message — exact, because outputs are written in input order. Several partitions:
 * the smallest such offset over all partitions, and none at all while any partition is empty. A
 * crash can then rewrite a few outputs (consumers dedupe by key) but can never skip one: an output
 * whose chunk failed sits above the minimum.
 */
async function lastWrittenSource(kafka: Kafka, admin: Admin, topic: string, partitions: number): Promise<bigint | undefined> {
  const tails = await readLastPerPartition(kafka, admin, topic);
  const sources = [...tails.values()].map((m) => m.headers[SOURCE_OFFSET_HEADER]).filter((x): x is string => x !== undefined).map((x) => BigInt(x));
  if (sources.length === 0) return undefined;
  if (partitions > 1 && sources.length < partitions) return undefined;
  return sources.reduce((a, b) => (b < a ? b : a));
}

export async function runStage(opts: StageRunOptions): Promise<StageRunStats> {
  const log = opts.log ?? (() => {});
  const replay = opts.fromOffset !== undefined;
  const kafka = new Kafka({ clientId: `wikipatrol-${opts.stage.name}`, brokers: opts.brokers, logLevel: logLevel.WARN });
  const admin = kafka.admin();
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
  const groupId = replay ? `wikipatrol-replay-${opts.stage.name}-${process.pid}-${randomUUID()}` : (opts.groupId ?? `wikipatrol-stage-${opts.stage.name}`);
  // A batch can wait on the Action API (enrich) for minutes when it is lagged; do not let the group
  // decide the consumer is dead meanwhile.
  const consumer = kafka.consumer({ groupId, sessionTimeout: 300_000, rebalanceTimeout: 300_000, heartbeatInterval: 10_000 });
  const stats: StageRunStats = { read: 0, written: 0, skippedAlreadyWritten: 0 };

  await admin.connect();
  try {
    // Source offsets order a single partition only; a partitioned input needs another runner
    // (the classifier's, src/classifier).
    const inputPartitions = (await admin.fetchTopicOffsets(opts.input)).length;
    if (inputPartitions !== 1) throw new Error(`${opts.input} has ${inputPartitions} partitions; a stage reads exactly one`);
    const partitions = opts.outputPartitions ?? 1;
    await ensureKeptTopic(admin, opts.output, partitions);
    // Highest input offset whose outputs are durably written — from the output topic at start, then
    // advanced after every write, so a redelivery after a mid-run rebalance is recognised too.
    let lastWritten = await lastWrittenSource(kafka, admin, opts.output, partitions);
    const alreadyWritten = (offset: string) => !replay && lastWritten !== undefined && BigInt(offset) <= lastWritten;
    log(`${opts.stage.name}: ${opts.input} -> ${opts.output}${replay ? ` (replay from ${opts.fromOffset}${opts.toOffset ? ` to ${opts.toOffset}` : ''})` : ` (group ${groupId}; last written source offset ${lastWritten ?? 'none'})`}`);

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: opts.input, fromBeginning: true });

    const stop = new AbortController();
    opts.signal?.addEventListener('abort', () => stop.abort(), { once: true });
    if (opts.signal?.aborted) stop.abort();
    let failed: unknown;
    const done = new Promise<void>((resolve) => {
      if (stop.signal.aborted) resolve();
      stop.signal.addEventListener('abort', () => resolve(), { once: true });
    });

    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning }) => {
        if (stop.signal.aborted || !isRunning()) return;
        let msgs = batch.messages.map((m) => ({ value: m.value?.toString() ?? '', offset: m.offset }));
        if (opts.toOffset !== undefined) msgs = msgs.filter((m) => BigInt(m.offset) <= BigInt(opts.toOffset!));
        if (msgs.length === 0) {
          if (opts.toOffset !== undefined) stop.abort();
          return;
        }
        let lastOffset: string;
        try {
          const results = await opts.stage.process(msgs);
          const out = results.flatMap((r) => {
            if (alreadyWritten(r.offset)) {
              stats.skippedAlreadyWritten += r.out.length;
              return [];
            }
            return r.out.map((o) => ({ key: o.key, value: o.value, headers: { [SOURCE_OFFSET_HEADER]: r.offset } }));
          });
          // In order, in chunks under the broker's request limit (a batch of large records is not).
          for (const chunk of chunkBySize(out)) await producer.send({ topic: opts.output, acks: -1, messages: chunk });
          const batchLast = BigInt(msgs.at(-1)!.offset);
          if (lastWritten === undefined || batchLast > lastWritten) lastWritten = batchLast;
          stats.read += msgs.length;
          stats.written += out.length;
          lastOffset = msgs.at(-1)!.offset;
          resolveOffset(lastOffset);
          opts.onBatch?.(out.length, lastOffset);
        } catch (err) {
          // Processing or writing failed: never let kafkajs retry the batch behind our back. Stop,
          // and let a restart resume from the committed offset.
          failed = err;
          stop.abort();
          return;
        }
        if (opts.toOffset !== undefined && BigInt(lastOffset) >= BigInt(opts.toOffset)) stop.abort();
        if (replay) return;
        try {
          // Outputs are durable before the input offset is committed: a crash can only re-read,
          // and re-read outputs are recognised by their source offset.
          await consumer.commitOffsets([{ topic: opts.input, partition: batch.partition, offset: String(BigInt(lastOffset) + 1n) }]);
          await heartbeat();
        } catch (err) {
          // A rebalance or a lost session is routine: the group re-forms and redelivers from the last
          // committed offset, and the source-offset check skips what was already written.
          log(`${opts.stage.name}: commit/heartbeat failed (${(err as { type?: string }).type ?? (err as Error).message}); continuing`);
        }
      },
    });
    if (replay) consumer.seek({ topic: opts.input, partition: 0, offset: opts.fromOffset! });
    await done;
    if (failed) throw failed;
  } finally {
    await consumer.disconnect();
    await producer.disconnect();
    await admin.disconnect();
  }
  log(`${opts.stage.name}: read ${stats.read}, wrote ${stats.written}${stats.skippedAlreadyWritten ? `, skipped ${stats.skippedAlreadyWritten} already written` : ''}`);
  return stats;
}
