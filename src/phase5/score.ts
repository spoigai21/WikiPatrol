// Phase 5: the table. One row per configuration that has a complete run on the set.
//
//   npm run phase5:score -- --set sealed

import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { args, log, writeJson } from '../phase0/cli.ts';
import { percentile, round, wilson } from '../phase0/stats.ts';
import type { SetEdit } from './build-set.ts';
import { loadSet, readRun, type RunRow } from './runner.ts';

export interface Score {
  config: string;
  modelVersions: string[];
  edits: number;
  complete: boolean;
  flagged: number;
  invalid: number;
  precision: ReturnType<typeof wilson>;
  recall: ReturnType<typeof wilson>;
  f1: number;
  latencyMs: { p50: number; p95: number } | null;
  tokensPerEdit: { in: number; out: number } | null;
}

/** Revert precision/recall over the set; invalid answers count as "not flagged" (D12). */
export function scoreRun(edits: readonly SetEdit[], rows: readonly RunRow[], config: string): Score {
  const byRev = new Map(rows.map((r) => [r.revId, r]));
  const scored = edits.filter((e) => e.label === 'reverted' || e.label === 'not-reverted');
  let tp = 0;
  let fp = 0;
  let fn = 0;
  for (const e of scored) {
    const flagged = byRev.get(e.revId)?.revert === true;
    const actual = e.label === 'reverted';
    if (flagged && actual) tp++;
    else if (flagged) fp++;
    else if (actual) fn++;
  }
  const precision = wilson(tp, tp + fp);
  const recall = wilson(tp, tp + fn);
  const f1 = tp === 0 ? 0 : (2 * tp) / (2 * tp + fp + fn);
  const lat = rows.map((r) => r.latencyMs).sort((a, b) => a - b);
  const withTokens = rows.filter((r) => r.tokensIn !== null && r.tokensOut !== null);
  return {
    config,
    modelVersions: [...new Set(rows.map((r) => r.modelVersion))],
    edits: rows.length,
    complete: edits.every((e) => byRev.has(e.revId)),
    flagged: round(rows.filter((r) => r.revert === true).length / Math.max(1, rows.length)),
    invalid: round(rows.filter((r) => r.revert === null).length / Math.max(1, rows.length)),
    precision,
    recall,
    f1: round(f1),
    latencyMs: lat.some((x) => x > 0) ? { p50: percentile(lat, 50), p95: percentile(lat, 95) } : null,
    tokensPerEdit: withTokens.length
      ? { in: Math.round(withTokens.reduce((a, r) => a + r.tokensIn!, 0) / withTokens.length), out: Math.round(withTokens.reduce((a, r) => a + r.tokensOut!, 0) / withTokens.length) }
      : null,
  };
}

const pct = (x: number) => (Number.isNaN(x) ? '—' : `${(x * 100).toFixed(1)}%`);
const ci = (w: ReturnType<typeof wilson>) => `${pct(w.rate)} [${pct(w.low)}, ${pct(w.high)}]`;

if (import.meta.main) {
  const opts = args({ set: { type: 'string', default: 'sealed' } });
  const set = String(opts.set);
  const edits = loadSet(set);
  const dir = `results/phase5/runs/${set}`;
  let files: string[] = [];
  try {
    files = readdirSync(dir).filter((f) => f.endsWith('.jsonl')).sort();
  } catch {
    /* no runs yet */
  }
  const scores = files.map((f) => {
    const rows = readRun(`${dir}/${f}`);
    return scoreRun(edits, rows, rows[0]?.config ?? f);
  });
  // Baselines first, then models, as the spec's table has them.
  scores.sort((a, b) => Number(!a.config.startsWith('baseline:')) - Number(!b.config.startsWith('baseline:')) || a.config.localeCompare(b.config));

  const reverted = edits.filter((e) => e.label === 'reverted').length;
  const decided = edits.filter((e) => e.label === 'reverted' || e.label === 'not-reverted').length;
  const noise = JSON.parse(readFileSync('results/phase0/label-noise.json', 'utf8')) as Record<string, any>;
  const report = {
    scoredAt: new Date().toISOString(),
    set,
    edits: edits.length,
    target: 'reverted within 72h (D9); population: edits that pass the default filter (D10, D12)',
    baseRate: wilson(reverted, decided),
    labelNoise: { labeller: noise.labeller, revertedNotVandalism: noise.revertedNotVandalism, keptButVandalism: noise.keptButVandalism },
    filterLoss: 'the filter itself drops 16–24% of reverted edits before any row here sees them (D10)',
    rows: scores,
  };
  writeJson(`results/phase5/grid-${set}.json`, report);

  const lines = [
    `# Phase 5 grid — ${set} set`,
    '',
    `${edits.length} edits that pass the filter; ${reverted} reverted within 72h (base rate ${ci(report.baseRate)}).`,
    `**Target is revert, not vandalism:** in the Phase 0 sample ${pct(noise.revertedNotVandalism.rate)} of reverted edits were not vandalism (${noise.labeller}).`,
    `The filter in front of every row already drops 16–24% of reverted edits (D10).`,
    '',
    '| Configuration | Precision | Recall | F1 | Flagged | Invalid | Latency p50 / p95 | Tokens in / out | Complete |',
    '|---|---|---|---|---|---|---|---|---|',
    ...scores.map((s) =>
      `| ${s.config} | ${ci(s.precision)} | ${ci(s.recall)} | ${s.f1.toFixed(3)} | ${pct(s.flagged)} | ${pct(s.invalid)} | ${s.latencyMs ? `${s.latencyMs.p50} / ${s.latencyMs.p95} ms` : '—'} | ${s.tokensPerEdit ? `${s.tokensPerEdit.in} / ${s.tokensPerEdit.out}` : '—'} | ${s.complete ? 'yes' : `${s.edits}/${edits.length}`} |`,
    ),
    '',
  ];
  writeFileSync(`results/phase5/grid-${set}.md`, lines.join('\n'));
  log(`wrote results/phase5/grid-${set}.{json,md} (${scores.length} rows)`);
}
