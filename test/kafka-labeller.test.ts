// Phase 4 done-criterion against a real Kafka API: labels arrive automatically from the raw log,
// and stopping the labeller mid-stream and restarting it changes nothing in what it writes.
// Runs only when KAFKA_BROKERS is set (npm run test:kafka).

import { Kafka, logLevel } from 'kafkajs';
import { afterEach, describe, expect, it } from 'vitest';
import { runKafkaLabeller } from '../src/labels/kafka-labeller.ts';
import { DEFAULT_LABELLER, Labeller, type Label } from '../src/labels/labeller.ts';
import { edit, tagged, ticks } from './events-fixture.ts';

const brokers = process.env.KAFKA_BROKERS?.split(',');
const H = 3_600_000;
const T0 = Date.parse('2026-09-30T12:00:00Z');

function rawEvents(): string[] {
  const events: string[] = [];
  for (let i = 0; i < 60; i++) {
    const at = T0 + i * 20 * 60_000;
    events.push(edit(5000 + i, at), ...ticks(at, at + 19 * 60_000));
    if (i % 4 === 0) events.push(tagged(5000 + i, at + 19 * 60_000, ['mw-reverted']));
  }
  events.push(...ticks(T0 + 20 * H, T0 + 100 * H));
  return events;
}

async function readLabels(kafka: Kafka, topic: string): Promise<Label[]> {
  const admin = kafka.admin();
  await admin.connect();
  const high = Number((await admin.fetchTopicOffsets(topic))[0]!.high);
  await admin.disconnect();
  const consumer = kafka.consumer({ groupId: `verify-${Date.now()}` });
  const out: Label[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await new Promise<void>((resolve) => {
    void consumer.run({
      eachMessage: async ({ message }) => {
        out.push(JSON.parse(message.value!.toString()) as Label);
        if (Number(message.offset) >= high - 1) resolve();
      },
    });
  });
  await consumer.disconnect();
  return out;
}

describe.skipIf(!brokers)('Kafka labeller (Redpanda)', () => {
  const kafka = brokers ? new Kafka({ brokers, logLevel: logLevel.NOTHING }) : undefined;
  const topics: string[] = [];
  afterEach(async () => {
    const admin = kafka!.admin();
    await admin.connect();
    await admin.deleteTopics({ topics: topics.splice(0) });
    await admin.disconnect();
  });

  it('labels the raw log, and a stop and restart writes exactly what one run would', async () => {
    const stamp = Date.now();
    const raw = `test.raw.lab.${stamp}`;
    const labels = `test.labels.${stamp}`;
    topics.push(raw, labels);

    const events = rawEvents();
    const produce = async (part: string[]) => {
      const producer = kafka!.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      for (let i = 0; i < part.length; i += 500) {
        await producer.send({ topic: raw, acks: -1, messages: part.slice(i, i + 500).map((value) => ({ value })) });
      }
      await producer.disconnect();
    };

    const expected: Label[] = [];
    const pure = new Labeller(DEFAULT_LABELLER);
    events.forEach((e, i) => expected.push(...pure.feed(e, String(i))));
    expect(expected).toHaveLength(60);

    // The log arrives in two parts. The first run stops once it has labelled everything the first
    // part can settle, with the rest of the edits still inside their window — the state a crash
    // or redeploy would throw away.
    const half = Math.floor(events.length * 0.75);
    // Edits that sit in the first part but are only settled by events in the second.
    const editsInFirstPart = expected.filter((l) => Number(l.sourceOffset) < half).length;
    const firstPart = new Labeller(DEFAULT_LABELLER);
    const expectedFirst = events.slice(0, half).flatMap((e, i) => firstPart.feed(e, String(i)));
    expect(expectedFirst.length).toBeGreaterThan(0);
    expect(expectedFirst.length).toBeLessThan(60);
    expect(editsInFirstPart).toBeGreaterThan(expectedFirst.length);

    const runUntil = async (total: number) => {
      const ctl = new AbortController();
      let written = 0;
      return runKafkaLabeller({
        brokers: brokers!, rawTopic: raw, labelTopic: labels, signal: ctl.signal,
        onWritten: (batch) => { written += batch.length; if (written >= total) ctl.abort(); },
      });
    };

    await produce(events.slice(0, half));
    const first = await runUntil(expectedFirst.length);
    expect(first.resumedFrom).toBeUndefined();
    expect(await readLabels(kafka!, labels)).toEqual(expectedFirst);

    await produce(events.slice(half));
    const second = await runUntil(60 - expectedFirst.length);
    expect(second.resumedFrom).toBe(expectedFirst.at(-1)!.sourceOffset);

    expect(await readLabels(kafka!, labels)).toEqual(expected);
  }, 120_000);
});
