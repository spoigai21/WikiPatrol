import { Kafka, logLevel, type Admin, type Producer } from 'kafkajs';
import { chunkBySize, ensureKeptTopic, KEEP_FOREVER, readTail } from '../kafka/topics.ts';
import type { RawRecord } from './envelope.ts';
import type { RawSink } from './sink.ts';

// One partition: total order, so an offset range is an unambiguous replay set.
// At ~1.1 classifiable edits/s (DECISIONS.md D1) one partition has huge headroom.
const PARTITIONS = 1;
const SSE_ID_HEADER = 'sse-id';
export const RAW_TOPIC_CONFIG = KEEP_FOREVER;

export interface KafkaSinkOptions {
  brokers: string[];
  topic: string;
  clientId?: string;
}

/**
 * Raw events into a Kafka/Redpanda topic, value = the data line byte-for-byte,
 * key = meta.id. Each message carries its own SSE id in a header, so the
 * checkpoint is simply the last message in the topic: a partially written batch
 * can only cause a replay on restart, never a gap.
 */
export class KafkaSink implements RawSink {
  private readonly kafka: Kafka;
  private readonly admin: Admin;
  private readonly producer: Producer;
  private ready: Promise<void> | undefined;

  constructor(private readonly opts: KafkaSinkOptions) {
    this.kafka = new Kafka({ clientId: opts.clientId ?? 'wikipatrol-ingester', brokers: opts.brokers, logLevel: logLevel.WARN });
    this.admin = this.kafka.admin();
    this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
  }

  private init(): Promise<void> {
    this.ready ??= (async () => {
      await this.admin.connect();
      await ensureKeptTopic(this.admin, this.opts.topic, PARTITIONS);
      await this.producer.connect();
    })();
    return this.ready;
  }

  async write(records: readonly RawRecord[], _lastEventId: string): Promise<void> {
    await this.init();
    if (records.length === 0) return;
    const messages = records.map((r) => ({
      key: r.id,
      value: r.data,
      headers: r.eventId === undefined ? {} : { [SSE_ID_HEADER]: r.eventId },
    }));
    // In order, in chunks under the broker's request limit. A crash between chunks leaves the last
    // written message as the checkpoint, so the rest is replayed and deduped (D7).
    for (const chunk of chunkBySize(messages)) await this.producer.send({ topic: this.opts.topic, acks: -1, messages: chunk });
  }

  async checkpoint(): Promise<string | undefined> {
    const [last] = (await this.tail(1)).slice(-1);
    return last?.eventId;
  }

  async recentIds(limit: number): Promise<string[]> {
    return (await this.tail(limit)).map((m) => m.id);
  }

  async close(): Promise<void> {
    if (!this.ready) return;
    await this.producer.disconnect();
    await this.admin.disconnect();
  }

  /** The last `n` messages of the topic, oldest first. */
  async tail(n: number): Promise<{ id: string; eventId: string | undefined; offset: string }[]> {
    await this.init();
    return (await readTail(this.kafka, this.admin, this.opts.topic, n)).map((m) => ({
      id: m.key,
      eventId: m.headers[SSE_ID_HEADER],
      offset: m.offset,
    }));
  }
}
