// Phases 3 and 4 on the live log: the labels the cluster's labeller has written to wiki.labels, and
// the filter's trade (volume removed, later-reverted edits lost) re-measured against them — the
// numbers Phase 3 could only estimate from Action API labels on replayed hours (D10, D11).
//
// Redpanda in the cluster advertises its in-cluster address only, so both topics are streamed out
// with `rpk topic consume` through kubectl rather than with a Kafka client on the host.
//
//   KUBECONFIG=~/.wikipatrol/kubeconfig npm run phase4:live

import { execFileSync, spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { readFileSync } from 'node:fs';
import { args, log, writeJson } from '../phase0/cli.ts';
import { round, summarise, wilson } from '../phase0/stats.ts';
import type { Label } from '../labels/labeller.ts';
import type { ScoredRecord } from '../pipeline/stages.ts';

const opts = args({ namespace: { type: 'string', default: 'wikipatrol' } });
const ns = String(opts.namespace);

function size(topic: string): number {
  const out = execFileSync('kubectl', ['-n', ns, 'exec', 'redpanda-0', '--', 'rpk', 'topic', 'describe', topic, '-p'], { encoding: 'utf8' });
  const lines = out.trim().split('\n');
  const col = lines[0]!.split(/\s+/).indexOf('HIGH-WATERMARK');
  return lines.slice(1).reduce((a, l) => a + Number(l.trim().split(/\s+/)[col]), 0);
}

/** Every message of a topic, as parsed JSON, streamed (topics run to hundreds of MB). */
async function consume<T>(topic: string, onValue: (v: T) => void): Promise<number> {
  const n = size(topic);
  if (n === 0) return 0;
  const p = spawn('kubectl', ['-n', ns, 'exec', 'redpanda-0', '--', 'rpk', 'topic', 'consume', topic, '-o', 'start', '-n', String(n), '-f', '%v\\n'], { stdio: ['ignore', 'pipe', 'inherit'] });
  let count = 0;
  for await (const line of createInterface({ input: p.stdout })) {
    if (!line) continue;
    onValue(JSON.parse(line) as T);
    count++;
  }
  return count;
}

if (import.meta.main) {
  // Labels first: they are the smaller set, and the join keeps only labelled edits.
  const labels = new Map<number, Label>();
  const nLabels = await consume<Label>('wiki.labels', (l) => labels.set(l.revId, l));
  log(`${nLabels} labels`);
  if (labels.size === 0) {
    log('no labels yet: the first edits in the log have not been 72 hours old long enough');
    process.exit(1);
  }

  const counts = { reverted: 0, 'not-reverted': 0, deleted: 0, incomplete: 0 } as Record<Label['label'], number>;
  for (const l of labels.values()) counts[l.label]++;
  const decided = counts.reverted + counts['not-reverted'];
  const delays = [...labels.values()].flatMap((l) => (l.revertDelaySeconds === undefined ? [] : [l.revertDelaySeconds]));

  // The filter's decision for every labelled edit (wiki.scored is keyed by rev id; at-least-once,
  // so a rev id can appear twice with the same decision — the first one is kept).
  const decision = new Map<number, ScoredRecord['filter']>();
  const nScored = await consume<ScoredRecord>('wiki.scored', (r) => {
    if (labels.has(r.revId) && !decision.has(r.revId)) decision.set(r.revId, r.filter);
  });
  const joined = [...labels.values()].filter((l) => decision.has(l.revId) && (l.label === 'reverted' || l.label === 'not-reverted'));
  const dropped = joined.filter((l) => !decision.get(l.revId)!.keep);
  const reverted = joined.filter((l) => l.label === 'reverted');
  const revertedDropped = reverted.filter((l) => !decision.get(l.revId)!.keep);
  const kept = joined.filter((l) => decision.get(l.revId)!.keep);

  const noise = JSON.parse(readFileSync('results/phase0/label-noise.json', 'utf8')) as Record<string, unknown>;
  const editTimes = [...labels.values()].map((l) => l.editTime).sort();
  const report = {
    measuredAt: new Date().toISOString(),
    source: 'the Phase 8 cluster: wiki.labels and wiki.scored (D16)',
    labels: {
      total: labels.size,
      editTimeRange: [editTimes[0], editTimes.at(-1)],
      byLabel: counts,
      revertRate: wilson(counts.reverted, decided),
      revertDelaySeconds: summarise(delays),
    },
    filter: {
      policy: [...decision.values()][0]?.policy ?? null,
      labelledEditsWithADecision: joined.length,
      volumeRemoved: round(dropped.length / joined.length),
      revertedLost: wilson(revertedDropped.length, reverted.length),
      revertRateBefore: round(reverted.length / joined.length),
      revertRateAfter: round(kept.filter((l) => l.label === 'reverted').length / kept.length),
      scoredMessagesRead: nScored,
    },
    labelNoise: { note: 'A revert is not vandalism; read every rate here beside these.', labeller: noise.labeller, revertedNotVandalism: noise.revertedNotVandalism, keptButVandalism: noise.keptButVandalism },
  };
  writeJson('results/phase4/live-labels.json', report);
  log(`${labels.size} labels (${editTimes[0]?.slice(0, 16)} to ${editTimes.at(-1)?.slice(0, 16)}); revert rate ${(report.labels.revertRate.rate * 100).toFixed(1)}%; filter removes ${(report.filter.volumeRemoved * 100).toFixed(1)}%, loses ${(report.filter.revertedLost.rate * 100).toFixed(1)}% of reverted -> results/phase4/live-labels.json`);
}
