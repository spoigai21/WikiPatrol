// Every stage after the ingester (D13). Each stage is its own consumer group, so they run equally
// well together (the laptop and `docker compose` shape) or one per deployment (Kubernetes, Phase 8).
//
//   npm run pipeline                        all stages in one process
//   npm run pipeline -- --stages filter     just one (a Kubernetes Deployment runs it like this)

import { parseArgs } from 'node:util';
import { Kafka, logLevel } from 'kafkajs';
import { runStage, STAGE_GROUP_SETTLE_MS, type Stage } from '../kafka/stage.ts';
import { runKafkaLabeller } from '../labels/kafka-labeller.ts';
import { Health, Progress } from '../ops/health.ts';
import { dlqStage, enrichStage, filterStage, parseStage, SCORED_PARTITIONS, TOPICS } from './stages.ts';

const ALL = ['parse', 'dlq', 'enrich', 'filter', 'labeller'] as const;
type Name = (typeof ALL)[number];

const { values } = parseArgs({
  options: {
    brokers: { type: 'string', default: process.env.KAFKA_BROKERS ?? 'localhost:19092' },
    stages: { type: 'string', default: process.env.STAGES ?? ALL.join(',') },
  },
});
const brokers = String(values.brokers).split(',');
const wanted = String(values.stages).split(',').map((s) => s.trim()) as Name[];
for (const w of wanted) if (!(ALL as readonly string[]).includes(w)) throw new Error(`unknown stage "${w}" (one of ${ALL.join(', ')})`);

const log = (msg: string) => process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
const ctl = new AbortController();
for (const sig of ['SIGINT', 'SIGTERM'] as const) process.once(sig, () => ctl.abort());

// Health: a stage is live while it makes progress, or while its input has nothing new for it.
const admin = new Kafka({ clientId: 'wikipatrol-pipeline-health', brokers, logLevel: logLevel.NOTHING }).admin();
await admin.connect();
const highWatermark = async (topic: string) => (await admin.fetchTopicOffsets(topic)).reduce((a, p) => a + BigInt(p.high), 0n);
const health = new Health();
const healthServer = health.serve();

function tracked(name: string, input: string) {
  let lastOffset = -1n;
  // Longer than the group can take to re-form after a hard stop, plus two minutes, or the probe
  // restarts a stage that is only waiting for its group (D16).
  const progress = new Progress(STAGE_GROUP_SETTLE_MS + 2 * 60_000, async () => (await highWatermark(input)) <= lastOffset + 1n);
  health.add({ name, live: () => progress.live(), ready: () => progress.ready() });
  return {
    started: () => progress.start(),
    advanced: (offset: string) => {
      lastOffset = BigInt(offset);
      progress.tick();
    },
  };
}

/**
 * Keep one stage running: if it stops on an error, log it and start it again after a backoff.
 * Every stage resumes from its own checkpoint, so a restart loses and duplicates nothing.
 * One stage failing never stops the others. (In Kubernetes the liveness probe plays this part.)
 */
async function supervise(name: string, run: () => Promise<unknown>): Promise<void> {
  let backoff = 5_000;
  while (!ctl.signal.aborted) {
    const started = Date.now();
    try {
      await run();
      if (ctl.signal.aborted) return;
    } catch (err) {
      log(`${name} failed: ${err instanceof Error ? err.message : String(err)}`);
    }
    if (Date.now() - started > 10 * 60_000) backoff = 5_000;
    log(`${name}: restarting in ${backoff / 1000}s`);
    await new Promise((r) => setTimeout(r, backoff));
    backoff = Math.min(backoff * 2, 60_000);
  }
}

const stages: Record<Exclude<Name, 'labeller'>, [string, string, Stage]> = {
  parse: [TOPICS.raw, TOPICS.edits, parseStage()],
  dlq: [TOPICS.raw, TOPICS.dlq, dlqStage()],
  enrich: [TOPICS.edits, TOPICS.enriched, enrichStage()],
  filter: [TOPICS.enriched, TOPICS.scored, filterStage()],
};

await Promise.all(
  wanted.map((name) => {
    if (name === 'labeller') {
      const t = tracked('labeller', TOPICS.raw);
      return supervise('labeller', () => {
        t.started();
        return runKafkaLabeller({ brokers, rawTopic: TOPICS.raw, labelTopic: TOPICS.labels, signal: ctl.signal, log, onRead: t.advanced });
      });
    }
    const [input, output, stage] = stages[name];
    const t = tracked(name, input);
    // wiki.scored is the one partitioned topic: the classifier scales out over it (D14).
    const outputPartitions = output === TOPICS.scored ? SCORED_PARTITIONS : 1;
    return supervise(name, () => {
      t.started();
      return runStage({ brokers, input, output, outputPartitions, stage, signal: ctl.signal, log, onBatch: (_w, last) => t.advanced(last) });
    });
  }),
);
healthServer.close();
await admin.disconnect();
