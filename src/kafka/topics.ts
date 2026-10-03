// Topic helpers shared by every producer in the pipeline.

import { ConfigResourceTypes, type Admin, type Kafka } from 'kafkajs';

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
export async function ensureKeptTopic(admin: Admin, topic: string, partitions = 1): Promise<void> {
  if (!(await admin.listTopics()).includes(topic)) {
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: partitions, configEntries: [...KEEP_FOREVER] }] });
    return;
  }
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

/** The last `n` messages of partition 0, oldest first. */
export async function readTail(kafka: Kafka, admin: Admin, topic: string, n: number): Promise<TailMessage[]> {
  const offsets = await admin.fetchTopicOffsets(topic);
  const p0 = offsets.find((o) => o.partition === 0);
  const high = Number(p0?.high ?? 0);
  const low = Number(p0?.low ?? 0);
  const start = Math.max(low, high - n);
  if (high <= start) return [];

  const consumer = kafka.consumer({ groupId: `wikipatrol-tail-${process.pid}-${Date.now()}` });
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
