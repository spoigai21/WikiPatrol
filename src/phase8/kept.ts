// Phase 8: how much model work reached the classifier, minute by minute, over a recording (D16).
//
// The recording's feed is wiki.scored's growth: every edit, including the ones the filter drops
// for free. The replicas only spend time on the kept ones (one paced model call each), so the
// autoscaler should follow those. This reads each wiki.scored record's broker timestamp and filter
// decision over the recording's span, through kubectl, and writes per-minute counts beside it.
//
//   KUBECONFIG=… npm run phase8:kept -- --file results/phase8/diurnal-2026-10-05T1921Z.jsonl

import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { args, log, writeJson } from '../phase0/cli.ts';
import type { Sample } from './report.ts';

/** Per-minute arrivals at wiki.scored: [minute start (ms), all edits, kept edits]. */
export type KeptMinute = [number, number, number];

/** Count `<timestamp ms> <scored record JSON>` lines into minutes; unparseable lines are skipped. */
export function countKept(lines: Iterable<string>): KeptMinute[] {
  const by = new Map<number, [number, number]>();
  for (const line of lines) {
    const sp = line.indexOf(' ');
    if (sp < 0) continue;
    const ts = Number(line.slice(0, sp));
    let keep: boolean;
    try {
      keep = (JSON.parse(line.slice(sp + 1)) as { filter: { keep: boolean } }).filter.keep;
    } catch {
      continue;
    }
    if (!Number.isFinite(ts) || typeof keep !== 'boolean') continue;
    const m = Math.floor(ts / 60_000) * 60_000;
    const c = by.get(m) ?? [0, 0];
    c[0]++;
    if (keep) c[1]++;
    by.set(m, c);
  }
  return [...by].sort((a, b) => a[0] - b[0]).map(([m, [all, kept]]) => [m, all, kept]);
}

if (import.meta.main) {
  const opts = args({ file: { type: 'string' }, namespace: { type: 'string', default: 'wikipatrol' } });
  if (!opts.file) throw new Error('--file is required');
  const file = String(opts.file);
  const samples = readFileSync(file, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as Sample);
  // From 10 minutes before the first sample, so the first minutes have a trailing window.
  const from = new Date(Date.parse(samples[0]!.t) - 10 * 60_000).toISOString().slice(0, 19) + 'Z';
  const to = new Date(Date.parse(samples.at(-1)!.t) + 60_000).toISOString().slice(0, 19) + 'Z';
  const rpk = spawn('kubectl', ['-n', String(opts.namespace), 'exec', 'redpanda-0', '--', 'rpk', 'topic', 'consume', 'wiki.scored', '-o', `@${from}:${to}`, '-f', '%d %v\\n'], { stdio: ['ignore', 'pipe', 'inherit'] });
  const lines: string[] = [];
  for await (const l of createInterface({ input: rpk.stdout })) lines.push(l);
  const minutes = countKept(lines);
  const out = file.replace(/\.jsonl$/, '.kept.json');
  writeJson(out, { source: 'wiki.scored broker timestamps and filter.keep', from, to, records: lines.length, perMinute: minutes });
  log(`${lines.length} records, ${minutes.reduce((a, m) => a + m[2], 0)} kept, over ${minutes.length} minutes -> ${out}`);
}
