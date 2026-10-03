// The Phase 4 labeller: wiki.raw -> revert labels -> wiki.labels, continuously.
//
//   npm run labeller          (docker compose up -d; the ingester fills wiki.raw)

import { parseArgs } from 'node:util';
import { runKafkaLabeller } from './kafka-labeller.ts';

const { values } = parseArgs({
  options: {
    brokers: { type: 'string', default: process.env.KAFKA_BROKERS ?? 'localhost:19092' },
    raw: { type: 'string', default: 'wiki.raw' },
    labels: { type: 'string', default: 'wiki.labels' },
  },
});
const log = (msg: string) => process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
const ctl = new AbortController();
for (const sig of ['SIGINT', 'SIGTERM'] as const) process.once(sig, () => ctl.abort());

await runKafkaLabeller({
  brokers: String(values.brokers).split(','),
  rawTopic: String(values.raw),
  labelTopic: String(values.labels),
  signal: ctl.signal,
  log,
  onWritten: (batch) => log(`+${batch.length} labels (latest edit ${batch.at(-1)!.editTime})`),
});
