// Phase 10: a static dashboard built from the committed result files (no server, no live feed —
// a laptop cluster cannot keep one up, SPEC Phase 10). Rebuilt on every deploy, so new results
// appear as runs finish. It says plainly that it shows replayed data.
//
//   npm run dashboard:build      -> dashboard/dist/ (Vercel serves this)

import { copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { log } from '../phase0/cli.ts';
import { loadSet, readRun } from '../phase5/runner.ts';
import { scoreRun } from '../phase5/score.ts';

const OUT = 'dashboard/dist';
mkdirSync(`${OUT}/img`, { recursive: true });
const json = <T>(p: string): T | undefined => (existsSync(p) ? (JSON.parse(readFileSync(p, 'utf8')) as T) : undefined);
const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
/** "dev-2026-09-30T1200Z" -> "dev · 2026-09-30 12:00 UTC" */
const hourLabel = (name: string) => name.replace(/^(\w+)-(\d{4}-\d\d-\d\d)T(\d\d)(\d\d)Z$/, '$1 · $2 $3:$4 UTC');
const pct = (x: number | null | undefined, d = 1) => (x === null || x === undefined || Number.isNaN(x) ? '—' : `${(x * 100).toFixed(d)}%`);
const ci = (w: { rate: number; low: number; high: number }) => `${pct(w.rate)} <span class="ci">[${pct(w.low)}, ${pct(w.high)}]</span>`;

// Label noise, carried beside every accuracy number.
const noise = json<{ revertedNotVandalism: { rate: number; low: number; high: number }; keptButVandalism: { rate: number; low: number; high: number }; labeller: string }>('results/phase0/label-noise.json')!;

// The grid: scored from the run files at build time, so partial cloud runs show progress honestly.
const sealed = loadSet('sealed');
const runDir = 'results/phase5/runs/sealed';
const runs = readdirSync(runDir).filter((f) => f.endsWith('.jsonl')).map((f) => readRun(`${runDir}/${f}`)).filter((r) => r.length);
const scores = runs.map((rows) => scoreRun(sealed, rows, rows[0]!.config)).sort((a, b) => Number(!a.config.startsWith('baseline:')) - Number(!b.config.startsWith('baseline:')) || a.config.localeCompare(b.config));
const reverted = sealed.filter((e) => e.label === 'reverted').length;
const incomplete = scores.filter((x) => !x.complete).map((x) => x.config);
// Phase 7: the ladder, once scored.
type LadderPolicy = { policy: string; precision: { rate: number; low: number; high: number }; recall: { rate: number; low: number; high: number }; f1: number; escalated: number; usdPer1000ClassifiableEdits: number };
const ladder = json<{ prices: { retrievedAt: string }; ladders: { cloudModel: string; chosenOnDev: { localPrompt: string; cloudPrompt: string; band: { lo: number; hi: number } }; sealed: LadderPolicy[] }[] }>('results/phase7/ladder.json');

const filter = json<{ tables: Record<string, Record<string, { volumeRemoved: number; volumeRemovedNonBot: number; revertedLost: { rate: number; low: number; high: number } }>> }>('results/phase3/filter-eval.json')!;
const validation = ['dev-2026-09-30T1200Z', 'heldout-2026-09-30T0400Z'].map((n) => json<{ compared: number; agreement: { rate: number } ; confusion: Record<string, number> }>(`results/phase4/validation-${n}.json`)!);
const calib = json<{ configs: Record<string, { overall: { ece: number; brier: number; auroc: number } }> }>('results/phase6/sealed/calibration.json');
const bp = readdirSync('results/phase2').filter((f) => /^backpressure-.*\.json$/.test(f)).sort().at(-1);
const backpressure = bp ? json<{ peakLag: number; slowMinutes: number; slowTier: { ratePerMinute: number }; verification: { handled: number; ok: boolean } }>(`results/phase2/${bp}`) : undefined;
if (bp) copyFileSync(`results/phase2/${bp.replace('.json', '.svg')}`, `${OUT}/img/backpressure.svg`);
copyFileSync('docs/architecture.svg', `${OUT}/img/architecture.svg`);
const calibImgs: string[] = [];
if (existsSync('results/phase6/sealed')) {
  for (const f of readdirSync('results/phase6/sealed').filter((f) => f.endsWith('.svg')).sort()) {
    copyFileSync(`results/phase6/sealed/${f}`, `${OUT}/img/calib-${f}`);
    calibImgs.push(f);
  }
}
// Phase 8: the final autoscaling report, once one exists (interrupted recordings are never shown).
const p8 = existsSync('results/phase8') ? readdirSync('results/phase8').filter((f) => /^diurnal-.*\.report\.json$/.test(f)).sort().at(-1) : undefined;
const phase8 = p8 ? json<{ from: string; to: string; hoursSpanned: number; feedVsReplicas30min: number; replicasRange: { min: number; max: number }; gaps: { minutes: number }[]; quietestHour: { start: string; feed: number; replicas: number }; busiestHour: { start: string; feed: number; replicas: number } }>(`results/phase8/${p8}`) : undefined;
if (p8) copyFileSync(`results/phase8/${p8.replace('.report.json', '.svg')}`, `${OUT}/img/phase8.svg`);
// Phases 3–4 on the live log, once the cluster's labels exist.
const live = json<{ labels: { total: number; revertRate: { rate: number; low: number; high: number } }; filter: { volumeRemoved: number; revertedLost: { rate: number; low: number; high: number } } }>('results/phase4/live-labels.json');
const drift = existsSync('results/phase9/alerts.jsonl') ? readFileSync('results/phase9/alerts.jsonl', 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { at: string; degradedTest: number | null; config: string; alerts: string[] }) : [];
const thresholds = json<{ minAgreement: number }>('results/phase9/thresholds.json');

const grid = scores
  .map((s) => {
    const cal = calib?.configs[s.config]?.overall;
    return s.complete
      ? `<tr><td>${esc(s.config)}</td><td>${ci(s.precision)}</td><td>${ci(s.recall)}</td><td>${pct(s.flagged)}</td><td>${pct(s.invalid)}</td><td>${cal ? cal.auroc.toFixed(2) : '—'}</td><td>${cal ? cal.ece.toFixed(2) : '—'}</td></tr>`
      : `<tr class="pending"><td>${esc(s.config)}</td><td colspan="6">running — ${s.edits} of ${sealed.length} edits answered</td></tr>`;
  })
  .join('\n');

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WikiPatrol results</title>
<style>
  :root { color-scheme: light; --bg: #fcfcfb; --panel: #f1f0ec; --ink: #0b0b0b; --ink-2: #52514e; --line: #e4e3df; --accent: #2a78d6; --warn: #a15c00; }
  @media (prefers-color-scheme: dark) { :root { color-scheme: dark; --bg: #1a1a19; --panel: #242422; --ink: #ffffff; --ink-2: #c3c2b7; --line: #3a3a37; --accent: #3987e5; --warn: #e0a347; } }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink); font: 15px/1.5 system-ui, -apple-system, sans-serif; }
  main { max-width: 1040px; margin: 0 auto; padding: 24px 16px 64px; overflow-wrap: anywhere; }
  .scroll { max-width: 100%; }
  h1 { font-size: 26px; margin: 0 0 4px; }
  h2 { font-size: 18px; margin: 36px 0 8px; }
  .lede { color: var(--ink-2); margin: 0 0 16px; }
  .banner { background: var(--panel); border-left: 4px solid var(--warn); padding: 10px 14px; border-radius: 6px; margin: 16px 0; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(220px, 100%), 1fr)); gap: 12px; }
  .card { background: var(--panel); border-radius: 10px; padding: 14px; min-width: 0; }
  .big { font-size: 30px; font-weight: 700; }
  .card p { margin: 4px 0 0; color: var(--ink-2); font-size: 13px; }
  .scroll { overflow-x: auto; }
  table { border-collapse: collapse; width: 100%; font-size: 14px; }
  th, td { text-align: left; padding: 7px 8px; border-bottom: 1px solid var(--line); white-space: nowrap; }
  th { color: var(--ink-2); font-weight: 600; }
  .ci { color: var(--ink-2); font-size: 12px; }
  tr.pending td { color: var(--ink-2); font-style: italic; }
  img { max-width: 100%; height: auto; border-radius: 8px; background: var(--panel); }
  .figs { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(300px, 100%), 1fr)); gap: 12px; }
  a { color: var(--accent); }
  .muted { color: var(--ink-2); font-size: 13px; }
</style>
</head>
<body>
<main>
<h1>WikiPatrol</h1>
<p class="lede">What does an expensive model buy you on Wikipedia's edit stream, when you cannot afford to call it on every edit?</p>

<div class="banner"><strong>Replayed data, not a live feed.</strong> Every number here comes from Wikipedia edits replayed from the stream's history and frozen as snapshots, built from the files committed in the repository on ${new Date().toISOString().slice(0, 10)}. ${incomplete.length ? `Unfinished and not counted: ${incomplete.map(esc).join(', ')} (left on a free tier).` : 'Every configuration is complete.'}</div>

<h2>The target is revert, not vandalism</h2>
<div class="cards">
  <div class="card"><div class="big">${pct(noise.revertedNotVandalism.rate, 0)}</div><p>of reverted edits were not vandalism (95% CI ${pct(noise.revertedNotVandalism.low, 0)}–${pct(noise.revertedNotVandalism.high, 0)}). Read every accuracy number on this page beside this one.</p></div>
  <div class="card"><div class="big">${pct(reverted / sealed.length)}</div><p>of the 1,000 sealed edits that reach the models were reverted within 72 hours.</p></div>
  <div class="card"><div class="big">${validation.reduce((a, v) => a + Math.round(v.agreement.rate * v.compared), 0).toLocaleString('en-US')} / ${validation.reduce((a, v) => a + v.compared, 0).toLocaleString('en-US')}</div><p>revert labels computed from the event stream agree with the Wikipedia API.</p></div>
</div>
<p class="muted">Label-noise sample of 100 edits; labelled by ${esc(noise.labeller)}.</p>

<h2>The free filter</h2>
<div class="scroll"><table>
<tr><th>Hour</th><th>Policy</th><th>Edits removed</th><th>Non-bot removed</th><th>Reverted edits lost</th></tr>
${Object.entries(filter.tables).flatMap(([hour, pols]) => Object.entries(pols).map(([pol, s]) => `<tr><td>${esc(hourLabel(hour))}</td><td>${esc(pol)}</td><td>${pct(s.volumeRemoved)}</td><td>${pct(s.volumeRemovedNonBot)}</td><td>${ci(s.revertedLost)}</td></tr>`)).join('\n')}
</table></div>

<h2>The grid — sealed set, revert prediction</h2>
<p class="muted">Precision and recall with Wilson 95% intervals, on the 1,000 sealed edits that pass the filter (which itself drops 13–24% of reverted edits). AUROC and calibration error (ECE) from each model's stated probability.</p>
<div class="scroll"><table>
<tr><th>Configuration</th><th>Precision</th><th>Recall</th><th>Flags</th><th>Invalid</th><th>AUROC</th><th>ECE</th></tr>
${grid}
</table></div>

${ladder ? `<h2>The ladder: filter → local → cloud</h2><p class="muted">Chosen on 200 dev edits by rules written down first, scored once. $ per 1,000 classifiable edits at list prices retrieved ${esc(ladder.prices.retrievedAt)}. The pre-registered ladder did not beat the local model alone: the local model's confidence is too weak to say which edits to send up.</p>${ladder.ladders.slice(0, 1).map((l) => `<p class="muted">Cloud step ${esc(l.cloudModel)} (${esc(l.chosenOnDev.cloudPrompt)}); escalate when the local model's p_revert is in [${l.chosenOnDev.band.lo}, ${l.chosenOnDev.band.hi}).</p><div class="scroll"><table><tr><th>Policy</th><th>Precision</th><th>Recall</th><th>F1</th><th>Sent to cloud</th><th>$ / 1,000 edits</th></tr>${l.sealed.map((r) => `<tr><td>${esc(r.policy)}</td><td>${ci(r.precision)}</td><td>${ci(r.recall)}</td><td>${r.f1.toFixed(3)}</td><td>${pct(r.escalated)}</td><td>$${r.usdPer1000ClassifiableEdits.toFixed(3)}</td></tr>`).join('')}</table></div>`).join('')}` : ''}

${calibImgs.length ? `<h2>Does the model know when it is wrong?</h2><p class="muted">Stated probability of revert against the share actually reverted. On the diagonal is calibrated.</p><div class="figs">${calibImgs.map((f) => `<img src="img/calib-${f}" alt="Reliability diagram: ${esc(f.replace('.svg', ''))}">`).join('')}</div>` : ''}

${backpressure ? `<h2>Why Kafka: backpressure</h2><p class="muted">A model tier at ${backpressure.slowTier.ratePerMinute} edits a minute fell ${backpressure.peakLag.toLocaleString('en-US')} behind in ${backpressure.slowMinutes} minutes of live traffic; drained at full speed, ${backpressure.verification.handled.toLocaleString('en-US')} messages handled ${backpressure.verification.ok ? 'exactly once' : '— verification failed'}.</p><img src="img/backpressure.svg" alt="Consumer lag over time">` : ''}

${phase8 ? `<h2>Autoscaling on the real feed</h2><p class="muted">${phase8.hoursSpanned.toFixed(1)} hours (${esc(phase8.from.slice(0, 16).replace('T', ' '))} to ${esc(phase8.to.slice(0, 16).replace('T', ' '))} UTC${phase8.gaps.length ? `; ${phase8.gaps.reduce((a, g) => a + g.minutes, 0)} minutes unrecorded, shown as breaks` : ''}). Replicas ${phase8.replicasRange.min}–${phase8.replicasRange.max}; quietest hour ${Math.round(phase8.quietestHour.feed)} edits/min at ${phase8.quietestHour.replicas.toFixed(1)} replicas on average, busiest ${Math.round(phase8.busiestHour.feed)} at ${phase8.busiestHour.replicas.toFixed(1)}; feed vs replicas over 30-minute windows r = ${Number.isNaN(phase8.feedVsReplicas30min) ? '—' : phase8.feedVsReplicas30min}.</p><img src="img/phase8.svg" alt="Feed, replicas and lag over the day">` : ''}

${live ? `<h2>Labels from the live log</h2><div class="cards"><div class="card"><div class="big">${live.labels.total.toLocaleString('en-US')}</div><p>edits labelled automatically 72 hours after they were made; ${ci(live.labels.revertRate)} reverted.</p></div><div class="card"><div class="big">${pct(live.filter.volumeRemoved)}</div><p>of them the filter removed, losing ${ci(live.filter.revertedLost)} of the reverted ones.</p></div></div>` : ''}

<h2>Drift</h2>
<p class="muted">Nightly reruns of 50 fixed sealed edits per configuration; alert below ${pct(thresholds?.minAgreement ?? 0.9, 0)} agreement with its own sealed answers.</p>
${drift.length ? `<div class="scroll"><table><tr><th>When</th><th>Configuration</th><th>Alert</th></tr>${drift.slice(-10).reverse().map((d) => `<tr><td>${esc(d.at.slice(0, 16).replace('T', ' '))}</td><td>${esc(d.config)}${d.degradedTest ? ` <span class="ci">(deliberately degraded ${d.degradedTest}%)</span>` : ''}</td><td>${esc(d.alerts.join('; '))}</td></tr>`).join('')}</table></div>` : '<p class="muted">No alerts.</p>'}

<h2>How it works</h2>
<img src="img/architecture.svg" alt="Architecture: live pipeline through Kafka topics, and offline evaluation">

<p class="muted" style="margin-top:32px">Source, decisions and postmortem: <a href="https://github.com/spoigai21/WikiPatrol">github.com/spoigai21/WikiPatrol</a>. Costs, when reported, come from dated published list prices, never from the free tiers the runs used.</p>
</main>
</body>
</html>
`;
writeFileSync(`${OUT}/index.html`, html);
log(`wrote ${OUT}/index.html (${scores.length} grid rows, ${calibImgs.length} calibration charts)`);
