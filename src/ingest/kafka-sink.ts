import { Kafka, logLevel, type Admin, type Producer } from 'kafkajs';
import type { RawRecord } from './envelope.ts';
import type { RawSink } from './sink.ts';

// One partition: total order, so an offset range is an unambiguous replay set.
// At ~1.1 classifiable edits/s (DECISIONS.md D1) one partition has huge headroom.
const PARTITIONS = 1;
const SSE_ID_HEADER = 'sse-id';

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
      if (!(await this.admin.listTopics()).includes(this.opts.topic)) await this.admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: this.opts.topic,
            numPartitions: PARTITIONS,
            // Never age out: the dev and sealed offset ranges must exist for Phase 5 and Phase 9.
            configEntries: [
              { name: 'retention.ms', value: '-1' },
              { name: 'retention.bytes', value: '-1' },
              { name: 'cleanup.policy', value: 'delete' },
            ],
          },
        ],
      });
      await this.producer.connect();
    })();
    return this.ready;
  }

  async write(records: readonly RawRecord[], _lastEventId: string): Promise<void> {
    await this.init();
    if (records.length === 0) return;
    await this.producer.send({
      topic: this.opts.topic,
      acks: -1,
      messages: records.map((r) => ({
        key: r.id,
        value: r.data,
        headers: r.eventId === undefined ? {} : { [SSE_ID_HEADER]: r.eventId },
      })),
    });
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
    const offsets = await this.admin.fetchTopicOffsets(this.opts.topic);
    const p0 = offsets.find((o) => o.partition === 0);
    const high = Number(p0?.high ?? 0);
    const low = Number(p0?.low ?? 0);
    const start = Math.max(low, high - n);
    if (high <= start) return [];

    const consumer = this.kafka.consumer({ groupId: `wikipatrol-tail-${process.pid}-${Date.now()}` });
    const out: { id: string; eventId: string | undefined; offset: string }[] = [];
    await consumer.connect();
    try {
      await consumer.subscribe({ topic: this.opts.topic, fromBeginning: true });
      await new Promise<void>((resolve, reject) => {
        consumer
          .run({
            autoCommit: false,
            eachMessage: async ({ message }) => {
              if (Number(message.offset) < start) return;
              out.push({
                id: message.key?.toString() ?? '',
                eventId: message.headers?.[SSE_ID_HEADER]?.toString(),
                offset: message.offset,
              });
              if (Number(message.offset) >= high - 1) resolve();
            },
          })
          .catch(reject);
        consumer.seek({ topic: this.opts.topic, partition: 0, offset: String(start) });
      });
    } finally {
      await consumer.disconnect();
    }
    return out;
  }
}
