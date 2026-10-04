// Topic helpers shared by every producer in the pipeline.

import { randomUUID } from 'node:crypto';
import kafkajs, { type Admin, type Kafka } from 'kafkajs';

// kafkajs is CommonJS: Node's ESM loader cannot see this export by name (vitest can, which hid it).
const { ConfigResourceTypes } = kafkajs;

// Never age out: the dev and sealed offset ranges must exist for Phase 5 and Phase 9.
export const KEEP_FOREVER = [
  { name: 'retention.ms', value: '-1' },
  { name: 'retention.bytes', value: '-1' },
  { name: 'cleanup.policy', value: 'delete' },
] as const;

/**
 * Create the topic with KEEP_FOREVER, or correct an existing one: a topic created some other way
 * (auto-create, by hand) gets the broker's default retention, which deletes data after days.
 */
/**
 * kafkajs's listTopics can fail with null metadata while another client is creating or deleting
 * topics at the same moment; it clears on a retry.
 */
async function listTopics(admin: Admin): Promise<string[]> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await admin.listTopics();
    } catch (err) {
      if (attempt >= 5 || !(err instanceof TypeError)) throw err;
      await new Promise((r) => setTimeout(r, 200 * (attempt + 1)));
    }
  }
}

export async function ensureKeptTopic(admin: Admin, topic: string, partitions = 1): Promise<void> {
  if (!(await listTopics(admin)).includes(topic)) {
    try {
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: partitions, configEntries: [...KEEP_FOREVER] }] });
      return;
    } catch {
      // Another process created it first (the ingester and the dlq stage both write wiki.dlq).
      // Fall through and make sure its retention is right.
      if (!(await listTopics(admin)).includes(topic)) throw new Error(`could not create topic ${topic}`);
    }
  }
  // A topic can grow partitions, never shrink: grow it if asked for more (wiki.scored, D14).
  const meta = await admin.fetchTopicMetadata({ topics: [topic] });
  const have = meta.topics[0]?.partitions.length ?? 0;
  if (have < partitions) await admin.createPartitions({ topicPartitions: [{ topic, count: partitions }] });
  const resource = { type: ConfigResourceTypes.TOPIC, name: topic };
  const { resources } = await admin.describeConfigs({
    resources: [{ ...resource, configNames: KEEP_FOREVER.map((c) => c.name) }],
    includeSynonyms: false,
  });
  const current = new Map(resources[0]?.configEntries.map((e) => [e.configName, e.configValue]));
  if (KEEP_FOREVER.every((c) => current.get(c.name) === c.value)) return;
  await admin.alterConfigs({ validateOnly: false, resources: [{ ...resource, configEntries: [...KEEP_FOREVER] }] });
}

export interface TailMessage {
  key: string;
  headers: Record<string, string | undefined>;
  offset: string;
}

/** The last message of every non-empty partition. */
export async function readLastPerPartition(kafka: Kafka, admin: Admin, topic: string): Promise<Map<number, TailMessage>> {
  const offsets = await admin.fetchTopicOffsets(topic);
  const want = offsets.filter((o) => BigInt(o.high) > BigInt(o.low));
  const out = new Map<number, TailMessage>();
  if (want.length === 0) return out;
  const consumer = kafka.consumer({ groupId: `wikipatrol-tail-${process.pid}-${randomUUID()}` });
  await consumer.connect();
  try {
    await consumer.subscribe({ topic, fromBeginning: true });
    await new Promise<void>((resolve, reject) => {
      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ partition, message }) => {
            const target = want.find((o) => o.partition === partition);
            if (!target || BigInt(message.offset) < BigInt(target.high) - 1n) return;
            out.set(partition, {
              key: message.key?.toString() ?? '',
              headers: Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()])),
              offset: message.offset,
            });
            if (out.size === want.length) resolve();
          },
        })
        .catch(reject);
      for (const o of want) consumer.seek({ topic, partition: o.partition, offset: String(BigInt(o.high) - 1n) });
    });
  } finally {
    await consumer.disconnect();
  }
  return out;
}

/** The last `n` messages of partition 0, oldest first. */
export async function readTail(kafka: Kafka, admin: Admin, topic: string, n: number): Promise<TailMessage[]> {
  const offsets = await admin.fetchTopicOffsets(topic);
  const p0 = offsets.find((o) => o.partition === 0);
  const high = Number(p0?.high ?? 0);
  const low = Number(p0?.low ?? 0);
  const start = Math.max(low, high - n);
  if (high <= start) return [];

  // Unique per call: two stages in one process starting in the same millisecond must not share a
  // group, or the single partition goes to one of them and the other waits forever.
  const consumer = kafka.consumer({ groupId: `wikipatrol-tail-${process.pid}-${randomUUID()}` });
  const out: TailMessage[] = [];
  await consumer.connect();
  try {
    await consumer.subscribe({ topic, fromBeginning: true });
    await new Promise<void>((resolve, reject) => {
      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ message }) => {
            if (Number(message.offset) < start) return;
            out.push({
              key: message.key?.toString() ?? '',
              headers: Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()])),
              offset: message.offset,
            });
            if (Number(message.offset) >= high - 1) resolve();
          },
        })
        .catch(reject);
      consumer.seek({ topic, partition: 0, offset: String(start) });
    });
  } finally {
    await consumer.disconnect();
  }
  return out;
}

/** Stay well under the broker's 1 MB request limit; kafkajs sends one record batch per call. */
const MAX_SEND_BYTES = 512 * 1024;

/**
 * Split messages into consecutive chunks whose payloads stay under MAX_SEND_BYTES, preserving
 * order. A single message over the limit gets a chunk to itself (and the broker decides).
 */
export function chunkBySize<T extends { key?: string | null; value: string | null }>(messages: readonly T[], maxBytes = MAX_SEND_BYTES): T[][] {
  const chunks: T[][] = [];
  let cur: T[] = [];
  let size = 0;
  for (const m of messages) {
    const n = Buffer.byteLength(m.value ?? '') + Buffer.byteLength(m.key ?? '') + 64;
    if (cur.length && size + n > maxBytes) {
      chunks.push(cur);
      cur = [];
      size = 0;
    }
    cur.push(m);
    size += n;
  }
  if (cur.length) chunks.push(cur);
  return chunks;
}
