// Same done-criterion as ingest.test.ts, against a real Kafka API (Redpanda).
// Runs only when KAFKA_BROKERS is set:  docker compose up -d && KAFKA_BROKERS=localhost:19092 npm test

import { Kafka, logLevel } from 'kafkajs';
import { afterEach, describe, expect, it } from 'vitest';
import { runIngester } from '../src/ingest/ingester.ts';
import { KafkaSink } from '../src/ingest/kafka-sink.ts';
import { expectedIds, N, payload, startFakeStream, type FakeStream } from './fake-stream.ts';

const brokers = process.env.KAFKA_BROKERS?.split(',');

async function readAll(topic: string) {
  const kafka = new Kafka({ brokers: brokers!, logLevel: logLevel.WARN });
  const admin = kafka.admin();
  await admin.connect();
  const high = Number((await admin.fetchTopicOffsets(topic))[0]!.high);
  await admin.disconnect();
  const consumer = kafka.consumer({ groupId: `verify-${Date.now()}` });
  const out: { key: string; value: string }[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await new Promise<void>((resolve) => {
    void consumer.run({
      eachMessage: async ({ message }) => {
        out.push({ key: message.key!.toString(), value: message.value!.toString() });
        if (Number(message.offset) >= high - 1) resolve();
      },
    });
  });
  await consumer.disconnect();
  return out;
}

async function waitForOffset(topic: string, n: number, ctl: AbortController, timeoutMs = 30_000) {
  const kafka = new Kafka({ brokers: brokers!, logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const start = Date.now();
  try {
    while (Date.now() - start < timeoutMs) {
      const offs = await admin.fetchTopicOffsets(topic).catch(() => []);
      if (Number(offs[0]?.high ?? 0) >= n) break;
      await new Promise((r) => setTimeout(r, 50));
    }
  } finally {
    await admin.disconnect();
    ctl.abort();
  }
}

describe.skipIf(!brokers)('KafkaSink (Redpanda)', () => {
  let fake: FakeStream;
  afterEach(() => fake?.server.close());

  it('survives random disconnects and a process restart: every event once, in order, byte-identical', async () => {
    fake = await startFakeStream(11);
    const topic = `test.raw.${Date.now()}`;
    const run = async (stopAt: number) => {
      const sink = new KafkaSink({ brokers: brokers!, topic });
      const ctl = new AbortController();
      const p = runIngester({ url: fake.url, sink, signal: ctl.signal, batchSize: 9, flushMs: 20, backoffMs: () => 0 });
      await waitForOffset(topic, stopAt, ctl);
      const stats = await p;
      await sink.close();
      return stats;
    };

    const first = await run(200); // first process, stopped partway
    expect(first.written).toBeLessThan(N);
    await run(N); // second process: fresh memory, continues from the topic itself

    const msgs = await readAll(topic);
    expect(msgs.map((m) => m.key)).toEqual(expectedIds);
    expect(msgs.map((m) => m.value)).toEqual(expectedIds.map((_, i) => payload(i + 1)));
  }, 90_000);
});
