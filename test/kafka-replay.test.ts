// Phase 2 done-criterion: replay from an arbitrary offset reproduces byte-identical output for the
// deterministic stages, and a live stage stopped and restarted writes every output exactly once.
// Runs only when KAFKA_BROKERS is set (npm run test:kafka).

import { Kafka, logLevel } from 'kafkajs';
import { afterEach, describe, expect, it } from 'vitest';
import { runStage, SOURCE_OFFSET_HEADER, type Stage } from '../src/kafka/stage.ts';
import { readTail } from '../src/kafka/topics.ts';
import { dlqStage, filterStage, parseStage } from '../src/pipeline/stages.ts';
import { edit, tagged, tick } from './events-fixture.ts';

const brokers = process.env.KAFKA_BROKERS?.split(',');
const T0 = Date.parse('2026-09-30T12:00:00Z');

function rawEvents(n: number): string[] {
  const out: string[] = [];
  for (let i = 0; i < n; i++) {
    const at = T0 + i * 1000;
    if (i % 7 === 3) out.push('{broken');
    else if (i % 5 === 0) out.push(tagged(9000 + i, at, ['mw-reverted']));
    else if (i % 3 === 0) out.push(tick(at));
    else out.push(edit(9000 + i, at, i % 4 === 0 ? { user: '~2026-1-1' } : {}));
  }
  return out;
}

interface Msg { key: string; value: string; source: string }

describe.skipIf(!brokers)('pipeline replay (Redpanda)', () => {
  const kafka = brokers ? new Kafka({ brokers, logLevel: logLevel.NOTHING }) : undefined;
  const topics: string[] = [];
  const name = (s: string) => { const t = `test.${s}.${Date.now()}.${Math.random().toString(36).slice(2, 6)}`; topics.push(t); return t; };

  afterEach(async () => {
    const admin = kafka!.admin();
    await admin.connect();
    await admin.deleteTopics({ topics: topics.splice(0) }).catch(() => {});
    await admin.disconnect();
  });

  async function produce(topic: string, values: string[]) {
    const p = kafka!.producer({ idempotent: true, maxInFlightRequests: 1 });
    await p.connect();
    for (let i = 0; i < values.length; i += 200) await p.send({ topic, acks: -1, messages: values.slice(i, i + 200).map((value) => ({ value })) });
    await p.disconnect();
  }

  async function readAll(topic: string): Promise<Msg[]> {
    const admin = kafka!.admin();
    await admin.connect();
    const high = Number((await admin.fetchTopicOffsets(topic))[0]!.high);
    await admin.disconnect();
    if (high === 0) return [];
    const c = kafka!.consumer({ groupId: `read-${Date.now()}-${Math.random()}` });
    const out: Msg[] = [];
    await c.connect();
    await c.subscribe({ topic, fromBeginning: true });
    await new Promise<void>((resolve) => {
      void c.run({ eachMessage: async ({ message }) => {
        out.push({ key: message.key!.toString(), value: message.value!.toString(), source: message.headers![SOURCE_OFFSET_HEADER]!.toString() });
        if (Number(message.offset) >= high - 1) resolve();
      } });
    });
    await c.disconnect();
    return out;
  }

  const replay = (input: string, output: string, stage: Stage, fromOffset: string, toOffset: string) =>
    runStage({ brokers: brokers!, input, output, stage, fromOffset, toOffset });

  it('replaying any offset range of a deterministic stage gives byte-identical output', async () => {
    const raw = name('raw');
    const events = rawEvents(400);
    await produce(raw, events);
    const last = String(events.length - 1);

    for (const make of [parseStage, dlqStage]) {
      const full = name('full');
      await replay(raw, full, make(), '0', last);
      const whole = await readAll(full);
      expect(whole.length).toBeGreaterThan(20);

      // Same range again, into a fresh topic: identical.
      const again = name('again');
      await replay(raw, again, make(), '0', last);
      expect(await readAll(again)).toEqual(whole);

      // An arbitrary middle range: exactly the matching slice of the full run.
      const mid = name('mid');
      await replay(raw, mid, make(), '137', '311');
      expect(await readAll(mid)).toEqual(whole.filter((m) => Number(m.source) >= 137 && Number(m.source) <= 311));
    }

    // The filter stage, replayed over recorded (enriched) input, is reproducible too.
    const enriched = name('enriched');
    const parsed = name('parsed');
    await replay(raw, parsed, parseStage(), '0', last);
    const withAccounts = (await readAll(parsed)).map((m, i) => {
      const r = JSON.parse(m.value);
      return JSON.stringify({ ...r, account: r.userClass === 'registered' ? { registration: '2020-01-01T00:00:00Z', editcount: i % 2 ? 900 : 5 } : null, accountAsOf: '2026-09-30T12:00:00.000Z' });
    });
    await produce(enriched, withAccounts);
    const f1 = name('f1');
    const f2 = name('f2');
    await replay(enriched, f1, filterStage(), '0', String(withAccounts.length - 1));
    await replay(enriched, f2, filterStage(), '0', String(withAccounts.length - 1));
    const a = await readAll(f1);
    expect(a).toHaveLength(withAccounts.length);
    expect(await readAll(f2)).toEqual(a);
    expect(new Set(a.map((m) => JSON.parse(m.value).filter.rule))).toEqual(new Set(['kept', 'trusted-account']));
  }, 180_000);

  it('several stages in one process can read the tails of their topics at the same moment', async () => {
    // Each stage reads the last message of its own output topic on start. Sharing a consumer group
    // across those reads left one of them waiting forever on a real pipeline start.
    const ts = Array.from({ length: 4 }, (_, i) => name(`tail${i}`));
    for (const t of ts) await produce(t, ['a', 'b', 'c']);
    const admin = kafka!.admin();
    await admin.connect();
    const reads = await Promise.race([
      Promise.all(ts.map((t) => readTail(kafka!, admin, t, 1))),
      new Promise<'timeout'>((r) => setTimeout(() => r('timeout'), 30_000)),
    ]);
    await admin.disconnect();
    expect(reads).not.toBe('timeout');
    expect((reads as { offset: string }[][]).map((r) => r[0]!.offset)).toEqual(['2', '2', '2', '2']);
  }, 60_000);

  it('a batch whose outputs exceed the broker request limit is written in chunks', async () => {
    const raw = name('raw');
    const out = name('out');
    // 3,000 inputs, 2 KB of output each: ~6 MB from what can arrive as a single batch.
    await produce(raw, Array.from({ length: 3000 }, (_, i) => String(i)));
    const big: Stage = { name: 'big', process: (msgs) => msgs.map((m) => ({ offset: m.offset, out: [{ key: m.value, value: 'z'.repeat(2048) }] })) };
    await replay(raw, out, big, '0', '2999');
    const rows = await readAll(out);
    expect(rows).toHaveLength(3000);
    expect(rows.map((r) => r.key)).toEqual(Array.from({ length: 3000 }, (_, i) => String(i)));
  }, 180_000);

  it('a stage writing a partitioned topic loses nothing across a restart', async () => {
    const raw = name('raw');
    const out = name('scored');
    const groupId = `test-part-${Date.now()}`;
    const events = rawEvents(400);
    const live = async (untilSource: number) => {
      const ctl = new AbortController();
      await runStage({ brokers: brokers!, input: raw, output: out, outputPartitions: 3, stage: parseStage(), signal: ctl.signal, groupId,
        onBatch: (_w, lastOffset) => { if (Number(lastOffset) >= untilSource) ctl.abort(); } });
    };
    await produce(raw, events.slice(0, 250));
    await live(249);
    await produce(raw, events.slice(250));
    await live(events.length - 1);

    const expected = name('expected');
    await replay(raw, expected, parseStage(), '0', String(events.length - 1));
    const want = new Set((await readAll(expected)).map((m) => m.key));
    const admin = kafka!.admin();
    await admin.connect();
    const parts = await admin.fetchTopicOffsets(out);
    await admin.disconnect();
    expect(parts).toHaveLength(3);
    // Read every partition: each expected edit is there (duplicates are allowed, losses are not).
    const c = kafka!.consumer({ groupId: `read-${Date.now()}` });
    const got = new Set<string>();
    const total = parts.reduce((a, p) => a + Number(p.high), 0);
    let seen = 0;
    await c.connect();
    await c.subscribe({ topic: out, fromBeginning: true });
    await new Promise<void>((resolve) => { void c.run({ eachMessage: async ({ message }) => { got.add(message.key!.toString()); if (++seen >= total) resolve(); } }); });
    await c.disconnect();
    expect([...want].filter((k) => !got.has(k))).toEqual([]);
    expect(got.size).toBe(want.size);
  }, 180_000);

  it('a stage refuses a partitioned input', async () => {
    const t = name('parted');
    const admin = kafka!.admin();
    await admin.connect();
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic: t, numPartitions: 2 }] });
    await admin.disconnect();
    await expect(runStage({ brokers: brokers!, input: t, output: name('o'), stage: parseStage(), fromOffset: '0', toOffset: '0' })).rejects.toThrow(/2 partitions/);
  }, 60_000);

  it('a live stage stopped mid-stream and restarted writes every output exactly once', async () => {
    const raw = name('raw');
    const out = name('out');
    const groupId = `test-parse-${Date.now()}`;
    const events = rawEvents(400);
    const live = async (untilSource: number) => {
      const ctl = new AbortController();
      await runStage({ brokers: brokers!, input: raw, output: out, stage: parseStage(), signal: ctl.signal, groupId,
        onBatch: (_w, lastOffset) => { if (Number(lastOffset) >= untilSource) ctl.abort(); } });
    };
    await produce(raw, events.slice(0, 250));
    await live(249);
    // The input offset is committed after each batch, so a restart does not start from zero.
    const admin0 = kafka!.admin();
    await admin0.connect();
    const [committed] = await admin0.fetchOffsets({ groupId, topics: [raw] });
    await admin0.disconnect();
    expect(committed!.partitions[0]!.offset).toBe('250');
    await produce(raw, events.slice(250));
    await live(events.length - 1);

    const expected = name('expected');
    await replay(raw, expected, parseStage(), '0', String(events.length - 1));
    const want = await readAll(expected);
    expect(await readAll(out)).toEqual(want);

    // A crash after outputs were written but before the input offset was committed means re-reading
    // what was already written. Simulate the worst case — the whole committed offset lost — and
    // check nothing is written twice.
    const admin = kafka!.admin();
    await admin.connect();
    await admin.resetOffsets({ groupId, topic: raw, earliest: true });
    await admin.disconnect();
    await live(events.length - 1);
    expect(await readAll(out)).toEqual(want);
  }, 180_000);
});
