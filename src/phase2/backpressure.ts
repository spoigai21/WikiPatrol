// Phase 2: the backpressure demo — the artifact that justifies Kafka (D1, D13).
//
// A consumer stands in for a rate-limited cloud model tier: every edit the filter keeps costs it
// 60/rate seconds (10/min = Groq's free tier, D12); dropped edits cost nothing. It starts at the
// head of wiki.scored while the live feed keeps arriving. Every few seconds the sampler records
// consumer lag = newest offset - committed offset, the signal Phase 8 autoscales on. After
// --minutes it switches to full speed, drains the backlog, and checks that every offset from
// where it started to where it stopped was handled exactly once.
//
//   npm run ingest & npm run pipeline &      (live feed through the stages)
//   npm run phase2:backpressure -- --minutes 20

import { writeFileSync } from 'node:fs';
import { Kafka, logLevel } from 'kafkajs';
import { args, log, stamp, writeJson } from '../phase0/cli.ts';
import { TOPICS, type ScoredRecord } from '../pipeline/stages.ts';

const opts = args({
  brokers: { type: 'string', default: process.env.KAFKA_BROKERS ?? 'localhost:19092' },
  topic: { type: 'string', default: TOPICS.scored },
  minutes: { type: 'string', default: '20' },
  rate: { type: 'string', default: '10' },
  'sample-seconds': { type: 'string', default: '15' },
  'max-drain-minutes': { type: 'string', default: '20' },
});
const topic = String(opts.topic);
const slowMs = 60_000 / Number(opts.rate);
const startedAt = new Date();
const groupId = `wikipatrol-slow-tier-${stamp(startedAt)}`;

const kafka = new Kafka({ clientId: 'wikipatrol-backpressure', brokers: String(opts.brokers).split(','), logLevel: logLevel.WARN });
const admin = kafka.admin();
await admin.connect();
const high = async () => BigInt((await admin.fetchTopicOffsets(topic))[0]!.high);
const startOffset = await high();

// Start at the head: the backlog in the graph is only what arrived while this ran.
await admin.setOffsets({ groupId, topic, partitions: [{ partition: 0, offset: String(startOffset) }] });

type Phase = 'slow' | 'drain';
let phase: Phase = 'slow';
const handled: bigint[] = [];
let kept = 0;
const samples: { t: string; elapsedS: number; phase: Phase; high: string; committed: string; lag: number; handled: number }[] = [];

const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 500 });
await consumer.connect();
await consumer.subscribe({ topic });
let committed = startOffset;
await consumer.run({
  autoCommit: false,
  partitionsConsumedConcurrently: 1,
  eachMessage: async ({ message, partition }) => {
    const rec = JSON.parse(message.value!.toString()) as ScoredRecord;
    if (rec.filter.keep) {
      kept++;
      if (phase === 'slow') await new Promise((r) => setTimeout(r, slowMs));
    }
    handled.push(BigInt(message.offset));
    committed = BigInt(message.offset) + 1n;
    await consumer.commitOffsets([{ topic, partition, offset: String(committed) }]);
  },
});

const sampleEvery = Number(opts['sample-seconds']) * 1000;
const t0 = Date.now();
const sample = async () => {
  const h = await high();
  samples.push({ t: new Date().toISOString(), elapsedS: Math.round((Date.now() - t0) / 1000), phase, high: String(h), committed: String(committed), lag: Number(h - committed), handled: handled.length });
  const s = samples.at(-1)!;
  log(`${s.phase} t+${s.elapsedS}s lag ${s.lag} (handled ${s.handled}, kept ${kept})`);
};
const timer = setInterval(() => void sample(), sampleEvery);
await new Promise((r) => setTimeout(r, Number(opts.minutes) * 60_000));
phase = 'drain';
log('switching to full speed to drain the backlog');
const drainDeadline = Date.now() + Number(opts['max-drain-minutes']) * 60_000;
while (Date.now() < drainDeadline) {
  await new Promise((r) => setTimeout(r, sampleEvery));
  if (samples.at(-1)?.phase === 'drain' && samples.at(-1)!.lag === 0) break;
}
clearInterval(timer);
await sample();
await consumer.disconnect();
const endOffset = committed;
await admin.disconnect();

// Nothing lost, nothing twice: the handled offsets are exactly startOffset .. endOffset-1.
const sorted = [...handled].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
const duplicates = sorted.length - new Set(sorted.map(String)).size;
const expected = Number(endOffset - startOffset);
const gaps: string[] = [];
for (let i = 0, want = startOffset; i < sorted.length; i++, want++) {
  if (sorted[i] !== want) {
    gaps.push(`expected ${want}, got ${sorted[i]}`);
    break;
  }
}
const verification = { startOffset: String(startOffset), endOffset: String(endOffset), handled: handled.length, expected, duplicates, gaps, ok: duplicates === 0 && gaps.length === 0 && handled.length === expected };
const peak = samples.reduce((a, s) => Math.max(a, s.lag), 0);

const name = `results/phase2/backpressure-${stamp(startedAt)}`;
writeJson(`${name}.json`, {
  startedAt: startedAt.toISOString(),
  topic,
  slowTier: { ratePerMinute: Number(opts.rate), msPerKeptEdit: slowMs, note: 'stands in for a free-tier cloud model (D12); dropped edits cost nothing' },
  slowMinutes: Number(opts.minutes),
  keptEdits: kept,
  peakLag: peak,
  verification,
  samples,
});
writeFileSync(`${name}.svg`, lagSvg(samples, Number(opts.minutes) * 60));
log(`peak lag ${peak}; ${verification.ok ? 'verified: every offset handled exactly once' : `VERIFICATION FAILED ${JSON.stringify(verification)}`} -> ${name}.{json,svg}`);

function lagSvg(points: typeof samples, slowSeconds: number): string {
  const W = 640, H = 300;
  const m = { l: 72, r: 16, t: 40, b: 44 };
  const pw = W - m.l - m.r, ph = H - m.t - m.b;
  const maxT = Math.max(1, ...points.map((p) => p.elapsedS));
  const rawMax = Math.max(1, ...points.map((p) => p.lag));
  const step = 10 ** Math.floor(Math.log10(rawMax));
  const maxY = Math.ceil(rawMax / step) * step;
  const x = (t: number) => m.l + (t / maxT) * pw;
  const y = (v: number) => m.t + (1 - v / maxY) * ph;
  const yTicks = [0, maxY / 2, maxY];
  const xTicks = [0, Math.round(maxT / 2), maxT];
  const pts = points.map((p) => `${x(p.elapsedS).toFixed(1)},${y(p.lag).toFixed(1)}`).join(' ');
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Consumer lag of a rate-limited tier">
<style>
  :root { --surface-1: #fcfcfb; --surface-2: #f1f0ec; --text-primary: #0b0b0b; --text-secondary: #52514e; --grid: #e4e3df; --series-1: #2a78d6; }
  @media (prefers-color-scheme: dark) { :root { --surface-1: #1a1a19; --surface-2: #242422; --text-primary: #ffffff; --text-secondary: #c3c2b7; --grid: #3a3a37; --series-1: #3987e5; } }
  text { font: 11px system-ui, sans-serif; fill: var(--text-secondary); }
  .title { font-size: 13px; font-weight: 600; fill: var(--text-primary); }
</style>
<rect width="${W}" height="${H}" fill="var(--surface-1)"/>
<text class="title" x="${m.l}" y="22">Consumer lag: a ${opts.rate}/min model tier behind the feed, then draining</text>
<rect x="${x(Math.min(slowSeconds, maxT))}" y="${m.t}" width="${Math.max(0, x(maxT) - x(Math.min(slowSeconds, maxT)))}" height="${ph}" fill="var(--surface-2)"/>
<text x="${x(Math.min(slowSeconds, maxT)) - 6}" y="${m.t + 30}" text-anchor="end">drain at full speed →</text>
<text x="${m.l + 6}" y="${m.t + 14}">rate-limited (${opts.rate} kept edits/min)</text>
${yTicks.map((v) => `<line x1="${m.l}" x2="${m.l + pw}" y1="${y(v)}" y2="${y(v)}" stroke="var(--grid)" stroke-width="1"/><text x="${m.l - 8}" y="${y(v) + 4}" text-anchor="end">${Math.round(v).toLocaleString('en-US')}</text>`).join('\n')}
${xTicks.map((t) => `<text x="${x(t)}" y="${H - m.b + 16}" text-anchor="middle">${Math.round(t / 60)} min</text>`).join('\n')}
<polyline fill="none" stroke="var(--series-1)" stroke-width="2" points="${pts}"/>
${points.map((p) => `<circle cx="${x(p.elapsedS).toFixed(1)}" cy="${y(p.lag).toFixed(1)}" r="6" fill="transparent"><title>${Math.round(p.elapsedS / 60)} min: lag ${p.lag} messages (${p.phase})</title></circle>`).join('\n')}
<text x="${m.l + pw / 2}" y="${H - 8}" text-anchor="middle">time since start</text>
<text x="16" y="${m.t + ph / 2}" text-anchor="middle" transform="rotate(-90 16 ${m.t + ph / 2})">messages waiting</text>
</svg>
`;
}
