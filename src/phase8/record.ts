// Phase 8: record the classifier's autoscaling against the real feed, once a minute (D16).
//
// Reads the HPA (replicas, the lag metric it scales on) and the topics' high watermarks and the
// classifier group's total lag, through kubectl, from outside the cluster. Appends one JSON line a
// minute, so a laptop sleep or a restart of this script loses at most the minutes it missed.
//
//   KUBECONFIG=… npm run phase8:record -- --hours 26

import { execFileSync } from 'node:child_process';
import { appendFileSync, mkdirSync } from 'node:fs';
import { args, log, stamp } from '../phase0/cli.ts';

const opts = args({
  hours: { type: 'string', default: '26' },
  every: { type: 'string', default: '60' },
  namespace: { type: 'string', default: 'wikipatrol' },
  out: { type: 'string' },
});
const ns = String(opts.namespace);
const out = String(opts.out ?? `results/phase8/diurnal-${stamp()}.jsonl`);
mkdirSync('results/phase8', { recursive: true });

const kubectl = (...a: string[]) => execFileSync('kubectl', ['-n', ns, ...a], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 60_000 });
const rpk = (...a: string[]) => kubectl('exec', 'redpanda-0', '--', 'rpk', ...a);

/** Sum of HIGH-WATERMARK over a topic's partitions. */
function high(topic: string): number | null {
  try {
    const lines = rpk('topic', 'describe', topic, '-p').trim().split('\n');
    const header = lines[0]!.split(/\s+/);
    const col = header.indexOf('HIGH-WATERMARK');
    return lines.slice(1).reduce((a, l) => a + Number(l.trim().split(/\s+/)[col]), 0);
  } catch {
    return null;
  }
}

function groupLag(group: string): number | null {
  try {
    const m = /TOTAL-LAG\s+(\d+)/.exec(rpk('group', 'describe', group, '-s'));
    return m ? Number(m[1]) : null;
  } catch {
    return null;
  }
}

function hpa(): { replicas: number | null; desired: number | null; metric: number | null } {
  try {
    const h = JSON.parse(kubectl('get', 'hpa', 'keda-hpa-classifier', '-o', 'json')) as {
      status?: { currentReplicas?: number; desiredReplicas?: number; currentMetrics?: { external?: { current?: { averageValue?: string } } }[] };
    };
    const v = h.status?.currentMetrics?.[0]?.external?.current?.averageValue;
    // Quantities like "11500m" are thousandths.
    const metric = v === undefined ? null : v.endsWith('m') ? Number(v.slice(0, -1)) / 1000 : Number(v);
    return { replicas: h.status?.currentReplicas ?? null, desired: h.status?.desiredReplicas ?? null, metric };
  } catch {
    return { replicas: null, desired: null, metric: null };
  }
}

const end = Date.now() + Number(opts.hours) * 3_600_000;
log(`recording every ${opts.every}s for ${opts.hours}h -> ${out}`);
while (Date.now() < end) {
  const t0 = Date.now();
  const row = {
    t: new Date().toISOString(),
    ...hpa(),
    classifierLag: groupLag('wikipatrol-classifier'),
    raw: high('wiki.raw'),
    scored: high('wiki.scored'),
    predictions: high('wiki.predictions'),
  };
  appendFileSync(out, JSON.stringify(row) + '\n');
  await new Promise((r) => setTimeout(r, Math.max(1000, Number(opts.every) * 1000 - (Date.now() - t0))));
}
log('done');
