// Phase 4: publish what the live labeller has produced, with the label-noise estimate beside it.
//
//   npm run phase4:summary            (reads the whole wiki.labels topic)

import { readFileSync } from 'node:fs';
import { Kafka, logLevel } from 'kafkajs';
import { args, log, writeJson } from '../phase0/cli.ts';
import { summarise, wilson } from '../phase0/stats.ts';
import type { Label } from '../labels/labeller.ts';

const opts = args({
  brokers: { type: 'string', default: process.env.KAFKA_BROKERS ?? 'localhost:19092' },
  topic: { type: 'string', default: 'wiki.labels' },
  out: { type: 'string', default: 'results/phase4/labels-summary.json' },
});

const kafka = new Kafka({ brokers: String(opts.brokers).split(','), logLevel: logLevel.WARN });
const admin = kafka.admin();
await admin.connect();
const topics = await admin.listTopics();
const high = topics.includes(String(opts.topic)) ? Number((await admin.fetchTopicOffsets(String(opts.topic)))[0]?.high ?? 0) : 0;
await admin.disconnect();

const labels: Label[] = [];
if (high > 0) {
  const consumer = kafka.consumer({ groupId: `wikipatrol-summary-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: String(opts.topic), fromBeginning: true });
  await new Promise<void>((resolve) => {
    void consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        labels.push(JSON.parse(message.value!.toString()) as Label);
        if (Number(message.offset) >= high - 1) resolve();
      },
    });
  });
  await consumer.disconnect();
}

const count = (v: Label['label']) => labels.filter((l) => l.label === v).length;
const reverted = count('reverted');
const decided = reverted + count('not-reverted');
const times = labels.map((l) => l.editTime).sort();
const noise = JSON.parse(readFileSync('results/phase0/label-noise.json', 'utf8')) as Record<string, unknown>;

writeJson(String(opts.out), {
  summarisedAt: new Date().toISOString(),
  topic: opts.topic,
  target: 'reverted within 72h (DECISIONS.md D9, D11)',
  labels: labels.length,
  editTimeRange: labels.length ? [times[0], times.at(-1)] : null,
  byLabel: { reverted, 'not-reverted': count('not-reverted'), deleted: count('deleted'), incomplete: count('incomplete') },
  // Deleted and incomplete labels are excluded: neither says whether the edit was reverted.
  revertRate: wilson(reverted, decided),
  revertDelaySeconds: summarise(labels.flatMap((l) => (l.revertDelaySeconds === undefined ? [] : [l.revertDelaySeconds]))),
  labelNoise: {
    note: 'A revert is not vandalism. Read every rate here beside these two numbers.',
    source: 'results/phase0/label-noise.json',
    labeller: noise.labeller,
    revertedNotVandalism: noise.revertedNotVandalism,
    keptButVandalism: noise.keptButVandalism,
  },
});
log(`${labels.length} labels in ${opts.topic}; revert rate ${decided ? ((reverted / decided) * 100).toFixed(2) + '%' : 'n/a'} -> ${opts.out}`);
