// Phase 8's scaling unit: classifier replicas share a partitioned wiki.scored as one group, and
// every edit gets a prediction. Runs only when KAFKA_BROKERS is set (npm run test:kafka).

import { Kafka, logLevel } from 'kafkajs';
import { afterEach, describe, expect, it } from 'vitest';
import { runClassifier, type PredictionRecord } from '../src/classifier/classifier.ts';
import { filterTemp, type Predictor } from '../src/phase5/predictors.ts';
import type { ScoredRecord } from '../src/pipeline/stages.ts';

const brokers = process.env.KAFKA_BROKERS?.split(',');

const scored = (revId: number): ScoredRecord => ({
  revId, wiki: 'enwiki', title: `P${revId}`, timestamp: 1_790_000_000, userClass: revId % 3 === 0 ? 'temporary' : 'registered',
  user: 'u', minor: false, isNew: false, lengthOld: 1, lengthNew: 2, comment: '', metaId: `m${revId}`,
  account: null, accountAsOf: null,
  filter: revId % 5 === 0 ? { policy: 'extendedconfirmed', keep: false, rule: 'trusted-account' } : { policy: 'extendedconfirmed', keep: true, rule: 'kept' },
});

describe.skipIf(!brokers)('classifier replicas (Redpanda)', () => {
  const kafka = brokers ? new Kafka({ brokers, logLevel: logLevel.NOTHING }) : undefined;
  const topics: string[] = [];
  afterEach(async () => {
    const admin = kafka!.admin();
    await admin.connect();
    await admin.deleteTopics({ topics: topics.splice(0) }).catch(() => {});
    await admin.disconnect();
  });

  it('two replicas in one group classify every edit of a partitioned topic', async () => {
    const stamp = `${Date.now()}`;
    const input = `test.scored.${stamp}`;
    const output = `test.predictions.${stamp}`;
    topics.push(input, output);
    const admin = kafka!.admin();
    await admin.connect();
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic: input, numPartitions: 3 }] });
    await admin.disconnect();
    const ids = Array.from({ length: 300 }, (_, i) => 1000 + i);
    const p = kafka!.producer();
    await p.connect();
    await p.send({ topic: input, messages: ids.map((id) => ({ key: String(id), value: JSON.stringify(scored(id)) })) });
    await p.disconnect();

    const ctl = new AbortController();
    const seen = new Set<string>();
    const partitionsUsed = new Map<string, Set<number>>();
    const groupId = `test-classifier-${stamp}`;
    const replica = (name: string) =>
      runClassifier({ brokers: brokers!, input, output, config: 'heuristic', groupId, signal: ctl.signal,
        onProcessed: (partition, offset) => {
          seen.add(`${partition}:${offset}`);
          (partitionsUsed.get(name) ?? partitionsUsed.set(name, new Set()).get(name)!).add(partition);
        } });
    const done = Promise.all([replica('a'), replica('b')]);
    const deadline = Date.now() + 60_000;
    // Batches report their last offset; stop once every partition is fully consumed.
    const highs = async () => {
      const ad = kafka!.admin(); await ad.connect();
      const [c] = await ad.fetchOffsets({ groupId, topics: [input] });
      const h = await ad.fetchTopicOffsets(input); await ad.disconnect();
      return h.every((x) => c?.partitions.find((q) => q.partition === x.partition)?.offset === x.high);
    };
    while (!(await highs()) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 500));
    ctl.abort();
    await done;

    const c = kafka!.consumer({ groupId: `read-${stamp}` });
    const preds = new Map<number, PredictionRecord>();
    const ad = kafka!.admin(); await ad.connect();
    const total = (await ad.fetchTopicOffsets(output)).reduce((a, x) => a + Number(x.high), 0); await ad.disconnect();
    let n = 0;
    await c.connect();
    await c.subscribe({ topic: output, fromBeginning: true });
    await new Promise<void>((resolve) => { void c.run({ eachMessage: async ({ message }) => { const r = JSON.parse(message.value!.toString()) as PredictionRecord; preds.set(r.revId, r); if (++n >= total) resolve(); } }); });
    await c.disconnect();

    expect([...preds.keys()].sort()).toEqual(ids);
    for (const id of ids) {
      const r = preds.get(id)!;
      if (id % 5 === 0) expect(r).toMatchObject({ tier: 'filter', revert: false });
      else expect(r).toMatchObject({ tier: 'model', revert: id % 3 === 0 });
    }
    expect([...partitionsUsed.values()].reduce((a, s) => a + s.size, 0)).toBe(3);
  }, 120_000);

  it('a replica evicted during a slow model call rejoins and finishes the topic', async () => {
    // D16: a model call outlasting the group session gets the replica evicted; its next commit then
    // fails ("the coordinator is not aware of this member"). That must send it back into the group,
    // not stop its consumer for good.
    const stamp = `${Date.now()}`;
    const input = `test.scored.${stamp}`;
    const output = `test.predictions.${stamp}`;
    topics.push(input, output);
    const admin = kafka!.admin();
    await admin.connect();
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic: input, numPartitions: 1 }] });
    await admin.disconnect();
    // All kept by the filter, so every edit is a model call.
    const ids = Array.from({ length: 36 }, (_, i) => 2000 + i).filter((id) => id % 5 !== 0);
    const p = kafka!.producer();
    await p.connect();
    await p.send({ topic: input, messages: ids.map((id) => ({ key: String(id), value: JSON.stringify(scored(id)) })) });
    await p.disconnect();

    let calls = 0;
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    // The third model call takes longer than the 6-second session.
    const slow: Predictor = { ...filterTemp, predict: async (e) => { if (++calls === 3) await sleep(12_000); return filterTemp.predict(e); } };
    const groupId = `test-classifier-${stamp}`;
    const logs: string[] = [];
    const ctl = new AbortController();
    const run = runClassifier({ brokers: brokers!, input, output, config: 'heuristic', predictor: slow, groupId, signal: ctl.signal, sessionTimeoutMs: 6_000, log: (m) => logs.push(m) });
    const consumedAll = async () => {
      const ad = kafka!.admin(); await ad.connect();
      const [c] = await ad.fetchOffsets({ groupId, topics: [input] });
      const h = await ad.fetchTopicOffsets(input); await ad.disconnect();
      return h.every((x) => c?.partitions.find((q) => q.partition === x.partition)?.offset === x.high);
    };
    const deadline = Date.now() + 90_000;
    while (!(await consumedAll()) && Date.now() < deadline) await sleep(500);
    const finished = await consumedAll();
    ctl.abort();
    await run;
    expect(logs.some((m) => m.includes('commit/heartbeat failed'))).toBe(true);
    expect(finished).toBe(true);
  }, 150_000);
});
