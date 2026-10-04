// The labeller as a service: wiki.raw -> Labeller -> wiki.labels.
//
// Checkpoint = the source offset carried by the last label written, exactly like the ingester
// (D7): labels are emitted in source-offset order, so resuming after that offset reproduces the
// rest of the stream of labels byte-for-byte. A crash mid-batch can only cause re-reading.

import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { chunkBySize, ensureKeptTopic, readTail } from '../kafka/topics.ts';
import { DEFAULT_LABELLER, Labeller, type Label, type LabellerOptions } from './labeller.ts';

export const SOURCE_OFFSET_HEADER = 'source-offset';

export interface KafkaLabellerOptions {
  brokers: string[];
  rawTopic: string;
  labelTopic: string;
  labeller?: LabellerOptions;
  signal?: AbortSignal;
  log?: (msg: string) => void;
  /** Called after each batch of labels is durably written (tests use it to stop mid-run). */
  onWritten?: (labels: readonly Label[]) => void;
  /** Called after each input batch is read, with its last offset (health probes). */
  onRead?: (lastOffset: string) => void;
}

export interface KafkaLabellerStats {
  resumedFrom: string | undefined;
  read: number;
  written: number;
}

export async function runKafkaLabeller(opts: KafkaLabellerOptions): Promise<KafkaLabellerStats> {
  const log = opts.log ?? (() => {});
  const kafka = new Kafka({ clientId: 'wikipatrol-labeller', brokers: opts.brokers, logLevel: logLevel.WARN });
  const admin = kafka.admin();
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
  // No consumer group state: the label topic itself is the checkpoint.
  const consumer = kafka.consumer({ groupId: `wikipatrol-labeller-${process.pid}-${randomUUID()}`, sessionTimeout: 300_000, rebalanceTimeout: 300_000, heartbeatInterval: 10_000 });
  const labeller = new Labeller(opts.labeller ?? DEFAULT_LABELLER);
  const stats: KafkaLabellerStats = { resumedFrom: undefined, read: 0, written: 0 };

  await admin.connect();
  try {
    await ensureKeptTopic(admin, opts.labelTopic);
    const [last] = await readTail(kafka, admin, opts.labelTopic, 1);
    stats.resumedFrom = last?.headers[SOURCE_OFFSET_HEADER];
    const start = stats.resumedFrom === undefined ? '0' : String(BigInt(stats.resumedFrom) + 1n);
    log(`labelling ${opts.rawTopic} from offset ${start} -> ${opts.labelTopic}`);

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: opts.rawTopic, fromBeginning: true });
    // A failed write must stop the service, not be retried: kafkajs would re-deliver the batch to
    // a labeller that has already absorbed it. Restarting resumes cleanly from the checkpoint.
    let failed: unknown;
    const stop = new AbortController();
    opts.signal?.addEventListener('abort', () => stop.abort(), { once: true });
    if (opts.signal?.aborted) stop.abort();
    const done = new Promise<void>((resolve) => {
      if (stop.signal.aborted) resolve();
      stop.signal.addEventListener('abort', () => resolve(), { once: true });
    });
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning }) => {
        const out: Label[] = [];
        for (const m of batch.messages) {
          if (!isRunning() || stop.signal.aborted) break;
          stats.read++;
          if (m.value) out.push(...labeller.feed(m.value.toString(), m.offset));
          resolveOffset(m.offset);
        }
        const lastRead = batch.messages.at(-1)?.offset;
        if (lastRead !== undefined) opts.onRead?.(lastRead);
        if (out.length === 0 || stop.signal.aborted) return;
        try {
          const messages = out.map((l) => ({ key: String(l.revId), value: JSON.stringify(l), headers: { [SOURCE_OFFSET_HEADER]: l.sourceOffset } }));
          for (const chunk of chunkBySize(messages)) await producer.send({ topic: opts.labelTopic, acks: -1, messages: chunk });
        } catch (err) {
          failed = err;
          stop.abort();
          return;
        }
        stats.written += out.length;
        opts.onWritten?.(out);
        try {
          await heartbeat();
        } catch (err) {
          // A rebalance would redeliver this batch to a labeller that already absorbed it. Stop
          // instead; a restart rebuilds the state from the checkpoint.
          failed = err;
          stop.abort();
        }
      },
    });
    consumer.seek({ topic: opts.rawTopic, partition: 0, offset: start });
    await done;
    if (failed) throw failed;
  } finally {
    await consumer.disconnect();
    await producer.disconnect();
    await admin.disconnect();
  }
  log(`stopped: read ${stats.read}, wrote ${stats.written} labels; ${labeller.pending} edits still in their window`);
  return stats;
}
