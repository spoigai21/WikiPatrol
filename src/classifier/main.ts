// The classifier as a service (Phase 8 scales it on consumer lag).
//
//   npm run classifier                               heuristic tier (no model; CI uses this)
//   CLASSIFIER_CONFIG=ollama:gemma3:4b:p2-guide npm run classifier

import { parseArgs } from 'node:util';
import { Kafka, logLevel } from 'kafkajs';
import { Health, Progress } from '../ops/health.ts';
import { TOPICS } from '../pipeline/stages.ts';
import { runClassifier } from './classifier.ts';

const { values } = parseArgs({
  options: {
    brokers: { type: 'string', default: process.env.KAFKA_BROKERS ?? 'localhost:19092' },
    config: { type: 'string', default: process.env.CLASSIFIER_CONFIG ?? 'heuristic' },
  },
});
const brokers = String(values.brokers).split(',');
const groupId = 'wikipatrol-classifier';
const log = (msg: string) => process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
const ctl = new AbortController();
for (const sig of ['SIGINT', 'SIGTERM'] as const) process.once(sig, () => ctl.abort());

// Live while classifying, or while this group has no lag. A replica stuck on a hung model call
// fails its probe and is restarted; its partitions go to the others meanwhile.
const admin = new Kafka({ clientId: 'wikipatrol-classifier-health', brokers, logLevel: logLevel.NOTHING }).admin();
await admin.connect();
const groupLag = async () => {
  const [committed] = await admin.fetchOffsets({ groupId, topics: [TOPICS.scored] });
  const highs = await admin.fetchTopicOffsets(TOPICS.scored);
  return highs.reduce((a, h) => {
    const c = committed?.partitions.find((p) => p.partition === h.partition)?.offset ?? '-1';
    return a + Number(BigInt(h.high) - (c === '-1' ? BigInt(h.low) : BigInt(c)));
  }, 0);
};
const progress = new Progress(5 * 60_000, async () => (await groupLag()) === 0);
const health = new Health();
health.add({ name: 'classifier', live: () => progress.live(), ready: () => progress.ready() });
const server = health.serve();
progress.start();

const pace = process.env.CLASSIFIER_PACE_MS ? { paceMs: Number(process.env.CLASSIFIER_PACE_MS) } : {};
await runClassifier({ brokers, input: TOPICS.scored, output: TOPICS.predictions, config: String(values.config), groupId, signal: ctl.signal, log, ...pace, onProcessed: () => progress.tick() });
server.close();
await admin.disconnect();
