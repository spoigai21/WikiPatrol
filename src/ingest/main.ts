// The Phase 1 ingester: Wikimedia EventStreams -> raw sink.
//
//   npm run ingest                                   # Kafka/Redpanda (docker compose up -d first)
//   npm run ingest -- --sink file --out data/raw      # no infrastructure

import { appendFileSync, mkdirSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { runIngester } from './ingester.ts';
import { KafkaSink } from './kafka-sink.ts';
import { FileSink, type RawSink } from './sink.ts';

const { values } = parseArgs({
  options: {
    sink: { type: 'string', default: 'kafka' },
    brokers: { type: 'string', default: process.env.KAFKA_BROKERS ?? 'localhost:19092' },
    topic: { type: 'string', default: 'wiki.raw' },
    out: { type: 'string', default: 'data/raw' },
    streams: { type: 'string', default: 'recentchange,mediawiki.revision-tags-change' },
  },
});

const out = String(values.out);
mkdirSync(out, { recursive: true });
const sink: RawSink =
  values.sink === 'file' ? new FileSink(out) : new KafkaSink({ brokers: String(values.brokers).split(','), topic: String(values.topic) });
const log = (msg: string) => process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);

const ctl = new AbortController();
for (const sig of ['SIGINT', 'SIGTERM'] as const) process.once(sig, () => ctl.abort());

const stats = await runIngester({
  url: `https://stream.wikimedia.org/v2/stream/${values.streams}`,
  sink,
  signal: ctl.signal,
  log,
  onInvalid: (data, reason) => appendFileSync(`${out}/invalid.jsonl`, JSON.stringify({ reason, data }) + '\n'),
});
await sink.close();
log(`stopped: ${JSON.stringify(stats)}`);
