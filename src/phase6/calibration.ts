// Phase 6: does the model know when it is wrong?
//
// For every run with real probabilities (an LLM's p_revert, LiftWing's score): a reliability
// table (stated probability of revert, in ten buckets, against how often those edits were really
// reverted), expected calibration error, Brier score, and AUROC — the last says whether ranking by
// p_revert separates reverted from kept edits at all, which is what routing on it needs. All of
// it overall and for temporary vs registered editors (SPEC Phase 6).
//
//   npm run phase6:calibration -- --set sealed

import { mkdirSync, readdirSync, writeFileSync } from 'node:fs';
import { args, log, writeJson } from '../phase0/cli.ts';
import { round, wilson } from '../phase0/stats.ts';
import type { SetEdit } from '../phase5/build-set.ts';
import { loadSet, readRun } from '../phase5/runner.ts';

export interface Scored {
  p: number;
  reverted: boolean;
}

export interface Bucket {
  from: number;
  to: number;
  n: number;
  meanP: number;
  observed: ReturnType<typeof wilson>;
}

const BUCKETS = 10;

export function reliability(items: readonly Scored[]): Bucket[] {
  const out: Bucket[] = [];
  for (let b = 0; b < BUCKETS; b++) {
    const from = b / BUCKETS;
    const to = (b + 1) / BUCKETS;
    // The last bucket is closed so p = 1 has a home.
    const inB = items.filter((x) => x.p >= from && (b === BUCKETS - 1 ? x.p <= to : x.p < to));
    if (inB.length === 0) continue;
    out.push({
      from,
      to,
      n: inB.length,
      meanP: round(inB.reduce((a, x) => a + x.p, 0) / inB.length),
      observed: wilson(inB.filter((x) => x.reverted).length, inB.length),
    });
  }
  return out;
}

/** Expected calibration error: bucket-weighted |observed revert rate - mean stated probability|. */
export function ece(items: readonly Scored[]): number {
  const n = items.length;
  if (n === 0) return NaN;
  return round(reliability(items).reduce((a, b) => a + (b.n / n) * Math.abs(b.observed.rate - b.meanP), 0));
}

export function brier(items: readonly Scored[]): number {
  if (items.length === 0) return NaN;
  return round(items.reduce((a, x) => a + (x.p - (x.reverted ? 1 : 0)) ** 2, 0) / items.length);
}

/** Probability that a random reverted edit gets a higher p than a random kept one (ties count half). */
export function auroc(items: readonly Scored[]): number {
  const sorted = [...items].sort((a, b) => a.p - b.p);
  const pos = sorted.filter((x) => x.reverted).length;
  const neg = sorted.length - pos;
  if (pos === 0 || neg === 0) return NaN;
  // Rank-sum (Mann-Whitney U) with average ranks for ties.
  let rankSumPos = 0;
  for (let i = 0; i < sorted.length; ) {
    let j = i;
    while (j < sorted.length && sorted[j]!.p === sorted[i]!.p) j++;
    const avgRank = (i + 1 + j) / 2;
    for (let k = i; k < j; k++) if (sorted[k]!.reverted) rankSumPos += avgRank;
    i = j;
  }
  return round((rankSumPos - (pos * (pos + 1)) / 2) / (pos * neg));
}

export function summarise(items: readonly Scored[]) {
  return {
    n: items.length,
    distinctP: new Set(items.map((x) => x.p)).size,
    ece: ece(items),
    brier: brier(items),
    auroc: auroc(items),
    reliability: reliability(items),
  };
}

/** A reliability diagram as a standalone SVG: stated probability (x) against observed rate (y). */
export function reliabilitySvg(title: string, buckets: readonly Bucket[]): string {
  const W = 360;
  const H = 360;
  const m = { l: 52, r: 16, t: 40, b: 48 };
  const pw = W - m.l - m.r;
  const ph = H - m.t - m.b;
  const x = (v: number) => m.l + v * pw;
  const y = (v: number) => m.t + (1 - v) * ph;
  const ticks = [0, 0.25, 0.5, 0.75, 1];
  const maxN = Math.max(1, ...buckets.map((b) => b.n));
  const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;');
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${esc(title)}">
<style>
  :root { --surface-1: #fcfcfb; --text-primary: #0b0b0b; --text-secondary: #52514e; --grid: #e4e3df; --series-1: #2a78d6; }
  @media (prefers-color-scheme: dark) { :root { --surface-1: #1a1a19; --text-primary: #ffffff; --text-secondary: #c3c2b7; --grid: #3a3a37; --series-1: #3987e5; } }
  text { font: 11px system-ui, sans-serif; fill: var(--text-secondary); }
  .title { font-size: 13px; font-weight: 600; fill: var(--text-primary); }
</style>
<rect width="${W}" height="${H}" fill="var(--surface-1)"/>
<text class="title" x="${m.l}" y="22">${esc(title)}</text>
${ticks.map((t) => `<line x1="${x(0)}" x2="${x(1)}" y1="${y(t)}" y2="${y(t)}" stroke="var(--grid)" stroke-width="1"/><text x="${m.l - 8}" y="${y(t) + 4}" text-anchor="end">${t}</text><text x="${x(t)}" y="${H - m.b + 16}" text-anchor="middle">${t}</text>`).join('\n')}
<line x1="${x(0)}" y1="${y(0)}" x2="${x(1)}" y2="${y(1)}" stroke="var(--text-secondary)" stroke-width="1" opacity="0.6"/>
<text x="${x(0.62)}" y="${y(0.7)}" transform="rotate(-45 ${x(0.62)} ${y(0.7)})">perfectly calibrated</text>
${buckets.length > 1 ? `<polyline fill="none" stroke="var(--series-1)" stroke-width="2" points="${buckets.map((b) => `${x(b.meanP)},${y(b.observed.rate)}`).join(' ')}"/>` : ''}
${buckets.map((b) => `<circle cx="${x(b.meanP)}" cy="${y(b.observed.rate)}" r="${(4 + 6 * Math.sqrt(b.n / maxN)).toFixed(1)}" fill="var(--series-1)" stroke="var(--surface-1)" stroke-width="2"><title>stated ${b.from.toFixed(1)}–${b.to.toFixed(1)}: ${b.n} edits, mean stated ${b.meanP}, reverted ${(b.observed.rate * 100).toFixed(1)}% [${(b.observed.low * 100).toFixed(1)}, ${(b.observed.high * 100).toFixed(1)}]</title></circle>`).join('\n')}
<text x="${m.l + pw / 2}" y="${H - 10}" text-anchor="middle">stated probability of revert (marker area = edits)</text>
<text x="14" y="${m.t + ph / 2}" text-anchor="middle" transform="rotate(-90 14 ${m.t + ph / 2})">share actually reverted</text>
</svg>
`;
}

if (import.meta.main) {
  const opts = args({ set: { type: 'string', default: 'sealed' } });
  const set = String(opts.set);
  const edits = new Map(loadSet(set).map((e) => [e.revId, e]));
  const dir = `results/phase5/runs/${set}`;
  const out = `results/phase6/${set}`;
  mkdirSync(out, { recursive: true });

  const configs: Record<string, unknown> = {};
  for (const f of readdirSync(dir).filter((f) => f.endsWith('.jsonl')).sort()) {
    const rows = readRun(`${dir}/${f}`);
    const config = rows[0]?.config ?? f;
    const complete = [...edits.keys()].every((r) => rows.some((x) => x.revId === r));
    const withP = rows.flatMap((r) => {
      const e = edits.get(r.revId);
      if (!e || r.pRevert === null || (e.label !== 'reverted' && e.label !== 'not-reverted')) return [];
      return [{ p: r.pRevert, reverted: e.label === 'reverted', userClass: e.userClass as SetEdit['userClass'] }];
    });
    // Only real probabilities: a rule that says 0 or 1 has nothing to calibrate.
    if (!complete || new Set(withP.map((x) => x.p)).size <= 2) continue;
    configs[config] = {
      invalidExcluded: rows.filter((r) => r.pRevert === null).length,
      overall: summarise(withP),
      temporary: summarise(withP.filter((x) => x.userClass === 'temporary')),
      registered: summarise(withP.filter((x) => x.userClass === 'registered')),
    };
    writeFileSync(`${out}/${config.replace(/[^\w.+-]+/g, '_')}.svg`, reliabilitySvg(config, reliability(withP)));
  }
  writeJson(`${out}/calibration.json`, {
    computedAt: new Date().toISOString(),
    set,
    note: 'Complete runs with more than two distinct probabilities. Invalid answers have no probability and are excluded (counted).',
    configs,
  });
  for (const [c, s] of Object.entries(configs) as [string, { overall: ReturnType<typeof summarise> }][]) {
    log(`${c.padEnd(50)} ECE ${s.overall.ece}  Brier ${s.overall.brier}  AUROC ${s.overall.auroc}  (${s.overall.distinctP} distinct p)`);
  }
}
