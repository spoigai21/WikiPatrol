// Phase 8: turn the once-a-minute recording into the result — does the replica count follow the
// real feed? (D16). Gaps in the recording (the recorder stopped; the laptop slept) are never
// bridged: a minute-to-minute rate is only computed across consecutive minutes.
//
//   npm run phase8:report -- --file results/phase8/diurnal-2026-10-05T1921Z.jsonl

import { readFileSync, writeFileSync } from 'node:fs';
import { args, log, writeJson } from '../phase0/cli.ts';
import { round } from '../phase0/stats.ts';

export interface Sample {
  t: string;
  replicas: number | null;
  desired: number | null;
  metric: number | null;
  classifierLag: number | null;
  raw: number | null;
  scored: number | null;
  predictions: number | null;
}

export interface Minute {
  t: number;
  /** Edits per minute reaching the classifier (wiki.scored growth since the previous minute). */
  feed: number;
  replicas: number;
  lag: number;
}

const MAX_STEP_S = 180;

/** Per-minute points; a step longer than 3 minutes (a gap) produces no point. */
export function toMinutes(samples: readonly Sample[]): Minute[] {
  const out: Minute[] = [];
  for (let i = 1; i < samples.length; i++) {
    const a = samples[i - 1]!;
    const b = samples[i]!;
    const dt = (Date.parse(b.t) - Date.parse(a.t)) / 1000;
    if (dt <= 0 || dt > MAX_STEP_S || a.scored === null || b.scored === null || b.replicas === null || b.classifierLag === null) continue;
    out.push({ t: Date.parse(b.t), feed: ((b.scored - a.scored) * 60) / dt, replicas: b.replicas, lag: b.classifierLag });
  }
  return out;
}

/** Means over fixed windows (e.g. 30 minutes), keeping only windows with enough minutes in them. */
export function windows(minutes: readonly Minute[], sizeMin: number, minCoverage = 0.8) {
  const size = sizeMin * 60_000;
  const groups = new Map<number, Minute[]>();
  for (const m of minutes) {
    const k = Math.floor(m.t / size) * size;
    (groups.get(k) ?? groups.set(k, []).get(k)!).push(m);
  }
  return [...groups]
    .filter(([, ms]) => ms.length >= sizeMin * minCoverage)
    .sort((a, b) => a[0] - b[0])
    .map(([start, ms]) => ({
      start: new Date(start).toISOString(),
      minutes: ms.length,
      feed: round(ms.reduce((a, m) => a + m.feed, 0) / ms.length),
      replicas: round(ms.reduce((a, m) => a + m.replicas, 0) / ms.length),
      maxLag: Math.max(...ms.map((m) => m.lag)),
    }));
}

/** Pearson correlation; NaN with fewer than three points or no variance. */
export function correlation(xs: readonly number[], ys: readonly number[]): number {
  const n = xs.length;
  if (n < 3 || ys.length !== n) return NaN;
  const mx = xs.reduce((a, x) => a + x, 0) / n;
  const my = ys.reduce((a, y) => a + y, 0) / n;
  let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < n; i++) {
    sxy += (xs[i]! - mx) * (ys[i]! - my);
    sxx += (xs[i]! - mx) ** 2;
    syy += (ys[i]! - my) ** 2;
  }
  return sxx === 0 || syy === 0 ? NaN : round(sxy / Math.sqrt(sxx * syy));
}

/** Three stacked panels on one time axis: feed, replicas, lag. One y-scale per panel. */
export function chartSvg(minutes: readonly Minute[], wins: ReturnType<typeof windows>, title: string): string {
  const W = 900, PH = 150, GAP = 34, top = 44, left = 64, right = 16, bottom = 40;
  const H = top + 3 * PH + 2 * GAP + bottom;
  const t0 = Math.min(...minutes.map((m) => m.t));
  const t1 = Math.max(...minutes.map((m) => m.t));
  const x = (t: number) => left + ((t - t0) / Math.max(1, t1 - t0)) * (W - left - right);
  const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;');
  const nice = (v: number) => {
    const step = 10 ** Math.floor(Math.log10(Math.max(1, v)));
    return Math.ceil(v / step) * step;
  };
  const panel = (i: number, label: string, pts: [number, number][], maxY: number, step: boolean) => {
    const y0 = top + i * (PH + GAP);
    const y = (v: number) => y0 + PH - (v / maxY) * PH;
    // Break the line wherever the recording has a gap.
    const segs: string[] = [];
    let cur: string[] = [];
    let prevT: number | undefined;
    let prevY: number | undefined;
    for (const [t, v] of pts) {
      if (prevT !== undefined && t - prevT > MAX_STEP_S * 1000) {
        segs.push(cur.join(' '));
        cur = [];
      }
      if (step && prevY !== undefined && cur.length) cur.push(`${x(t).toFixed(1)},${prevY.toFixed(1)}`);
      cur.push(`${x(t).toFixed(1)},${y(v).toFixed(1)}`);
      prevT = t;
      prevY = y(v);
    }
    segs.push(cur.join(' '));
    return `<text class="lbl" x="${left}" y="${y0 - 8}">${esc(label)}</text>
${[0, maxY / 2, maxY].map((v) => `<line x1="${left}" x2="${W - right}" y1="${y(v)}" y2="${y(v)}" stroke="var(--grid)"/><text x="${left - 8}" y="${y(v) + 4}" text-anchor="end">${Math.round(v).toLocaleString('en-US')}</text>`).join('')}
${segs.filter((s) => s.split(' ').length > 1).map((s) => `<polyline fill="none" stroke="var(--series-1)" stroke-width="2" points="${s}"/>`).join('\n')}`;
  };
  const feedPts = wins.length ? minutes.map((m) => [m.t, m.feed] as [number, number]) : [];
  // Smooth the noisy per-minute feed with a 15-minute trailing mean for readability.
  const smooth = feedPts.map(([t], i) => {
    const w = feedPts.slice(Math.max(0, i - 14), i + 1).filter(([tt]) => t - tt <= 15 * 60_000);
    return [t, w.reduce((a, [, v]) => a + v, 0) / w.length] as [number, number];
  });
  const hours: number[] = [];
  for (let h = Math.ceil(t0 / 3_600_000) * 3_600_000; h <= t1; h += 3 * 3_600_000) hours.push(h);
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${esc(title)}">
<style>
  :root { --surface-1: #fcfcfb; --text-primary: #0b0b0b; --text-secondary: #52514e; --grid: #e4e3df; --series-1: #2a78d6; }
  @media (prefers-color-scheme: dark) { :root { --surface-1: #1a1a19; --text-primary: #ffffff; --text-secondary: #c3c2b7; --grid: #3a3a37; --series-1: #3987e5; } }
  text { font: 11px system-ui, sans-serif; fill: var(--text-secondary); }
  .title { font-size: 13px; font-weight: 600; fill: var(--text-primary); }
  .lbl { font-size: 12px; fill: var(--text-primary); }
</style>
<rect width="${W}" height="${H}" fill="var(--surface-1)"/>
<text class="title" x="${left}" y="22">${esc(title)}</text>
${panel(0, 'Edits reaching the classifier, per minute (15-min mean)', smooth, nice(Math.max(...smooth.map(([, v]) => v), 1)), false)}
${panel(1, 'Classifier replicas (set by the HPA from consumer lag)', minutes.map((m) => [m.t, m.replicas]), 6, true)}
${panel(2, 'Consumer lag (messages waiting)', minutes.map((m) => [m.t, m.lag]), nice(Math.max(...minutes.map((m) => m.lag), 1)), false)}
${hours.map((h) => `<text x="${x(h)}" y="${H - 14}" text-anchor="middle">${new Date(h).toISOString().slice(11, 16)} UTC</text>`).join('\n')}
</svg>
`;
}

if (import.meta.main) {
  const opts = args({ file: { type: 'string' } });
  if (!opts.file) throw new Error('--file is required');
  const file = String(opts.file);
  const samples = readFileSync(file, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as Sample);
  const minutes = toMinutes(samples);
  const half = windows(minutes, 30);
  const hourly = windows(minutes, 60);
  const gaps: { from: string; to: string; minutes: number }[] = [];
  for (let i = 1; i < samples.length; i++) {
    const dt = (Date.parse(samples[i]!.t) - Date.parse(samples[i - 1]!.t)) / 1000;
    if (dt > MAX_STEP_S) gaps.push({ from: samples[i - 1]!.t, to: samples[i]!.t, minutes: Math.round(dt / 60) });
  }
  const span = (Date.parse(samples.at(-1)!.t) - Date.parse(samples[0]!.t)) / 3_600_000;
  const report = {
    file,
    from: samples[0]!.t,
    to: samples.at(-1)!.t,
    hoursSpanned: round(span),
    minutesRecorded: minutes.length,
    gaps,
    // Over 30-minute windows: does the replica count move with the feed?
    feedVsReplicas30min: correlation(half.map((w) => w.feed), half.map((w) => w.replicas)),
    replicasRange: { min: Math.min(...minutes.map((m) => m.replicas)), max: Math.max(...minutes.map((m) => m.replicas)) },
    quietestHour: hourly.reduce((a, w) => (w.feed < a.feed ? w : a), hourly[0]!),
    busiestHour: hourly.reduce((a, w) => (w.feed > a.feed ? w : a), hourly[0]!),
    hourly,
  };
  const out = file.replace(/\.jsonl$/, '');
  writeJson(`${out}.report.json`, report);
  writeFileSync(`${out}.svg`, chartSvg(minutes, half, `Phase 8: the classifier's replicas against the live feed (${report.from.slice(0, 16)} to ${report.to.slice(0, 16)} UTC)`));
  log(`${minutes.length} minutes over ${report.hoursSpanned} h; feed vs replicas (30-min) r = ${report.feedVsReplicas30min}; replicas ${report.replicasRange.min}-${report.replicasRange.max} -> ${out}.{report.json,svg}`);
}
