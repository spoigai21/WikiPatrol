// The Phase 1 ingester: Wikimedia EventStreams -> raw sink.
//
//   npm run ingest                                   # Kafka/Redpanda (docker compose up -d first)
//   npm run ingest -- --sink file --out data/raw      # no infrastructure

import { appendFileSync, mkdirSync } from 'node:fs';
import { Kafka, logLevel } from 'kafkajs';
import { ensureKeptTopic } from '../kafka/topics.ts';
import { Health, Progress } from '../ops/health.ts';
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
// Only the file sink (and its dead-letter file) writes locally; a container has no writable /app.
if (values.sink === 'file') mkdirSync(out, { recursive: true });
const sink: RawSink =
  values.sink === 'file' ? new FileSink(out) : new KafkaSink({ brokers: String(values.brokers).split(','), topic: String(values.topic) });
const log = (msg: string) => process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);

const ctl = new AbortController();
for (const sig of ['SIGINT', 'SIGTERM'] as const) process.once(sig, () => ctl.abort());

// Events the ingester cannot even key go to the dead-letter topic (or a file, without Kafka).
let dlq: ((data: string, reason: string) => void) | undefined;
let closeDlq = async () => {};
if (values.sink !== 'file') {
  const kafka = new Kafka({ clientId: 'wikipatrol-ingester-dlq', brokers: String(values.brokers).split(','), logLevel: logLevel.WARN });
  const admin = kafka.admin();
  await admin.connect();
  await ensureKeptTopic(admin, 'wiki.dlq');
  await admin.disconnect();
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();
  let chain = Promise.resolve();
  dlq = (data, reason) => {
    chain = chain.then(() =>
      producer.send({ topic: 'wiki.dlq', acks: -1, messages: [{ key: 'ingester', value: JSON.stringify({ reason: `ingester: ${reason}`, sourceOffset: null, raw: data }) }] }).then(() => {}),
    );
  };
  closeDlq = async () => {
    await chain;
    await producer.disconnect();
  };
}

// Live while events keep arriving: the all-wiki feed never idles for minutes (D8), so five quiet
// minutes means the stream or Kafka is stuck. Ready once the first batch is written.
const progress = new Progress(5 * 60_000);
const health = new Health();
health.add({ name: 'ingest', live: () => progress.live(), ready: () => progress.ready() });
const healthServer = health.serve();

const stats = await runIngester({
  url: `https://stream.wikimedia.org/v2/stream/${values.streams}`,
  sink,
  signal: ctl.signal,
  log,
  onInvalid: dlq ?? ((data, reason) => appendFileSync(`${out}/invalid.jsonl`, JSON.stringify({ reason, data }) + '\n')),
  onWritten: () => progress.tick(),
});
healthServer.close();
await sink.close();
await closeDlq();
log(`stopped: ${JSON.stringify(stats)}`);
