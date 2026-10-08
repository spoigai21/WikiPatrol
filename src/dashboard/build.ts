// Phase 10: a static results page built from the committed result files (no server, no live feed —
// a laptop cluster cannot keep one up, SPEC Phase 10). Rebuilt on every deploy, so new results
// appear as runs finish. Written for readers outside the field: each result leads with a plain
// sentence and a simple picture; the exact figures, with their intervals, sit under "details".
// It says plainly that it shows recorded data, not a live feed.
//
//   npm run dashboard:build      -> dashboard/dist/ (Vercel serves this)

import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { log } from '../phase0/cli.ts';
import { loadSet, readRun } from '../phase5/runner.ts';
import { scoreRun, type Score } from '../phase5/score.ts';

const OUT = 'dashboard/dist';
mkdirSync(`${OUT}/img`, { recursive: true });
const json = <T>(p: string): T | undefined => (existsSync(p) ? (JSON.parse(readFileSync(p, 'utf8')) as T) : undefined);
const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
/** "stream-dev-2026-09-30T1200Z" -> "2026-09-30, 12:00 UTC" */
const hourLabel = (name: string) => name.replace(/^.*?(\d{4}-\d\d-\d\d)T(\d\d)(\d\d)Z$/, '$1, $2:$3 UTC');
const pct = (x: number | null | undefined, d = 0) => (x === null || x === undefined || Number.isNaN(x) ? '—' : `${(x * 100).toFixed(d)}%`);
type Wilson = { rate: number; low: number; high: number };
const ci = (w: Wilson) => `${pct(w.rate, 1)} <span class="ci">${pct(w.low, 1)}–${pct(w.high, 1)}</span>`;

// The page's palette. Chart images are re-themed to it as they are copied (themeSvg), so every
// figure sits on the same dark surface whatever the reader's system setting.
const C = { bg: '#0a0a0a', card: '#181818', line: '#2b2b2b', ink: '#f5f5f1', ink2: '#a8a8a8', accent: '#a66bff', mark: '#8f5cff', green: '#46d369', red: '#ff5a5f', amber: '#f5c518' };

/** Copy a chart SVG, overriding its colour variables with this page's dark palette. */
function themeSvg(src: string, dest: string, relabel: Record<string, string> = {}): void {
  const vars = `--surface-1:${C.card};--surface-2:${C.bg};--text-primary:${C.ink};--text-secondary:${C.ink2};--grid:${C.line};--series-1:${C.mark};--surface:${C.card};--panel:${C.bg};--ink:${C.ink};--ink-2:${C.ink2};--line:${C.ink2};--topic:#1d1238;--topic-edge:${C.mark};--svc:${C.bg};--svc-edge:${C.ink2};--scale:${C.amber};`;
  const svg = readFileSync(src, 'utf8').replace('</style>', `  :root, :root:root { ${vars} }\n  text { font-family: Inter, system-ui, sans-serif; }\n</style>`);
  writeFileSync(dest, Object.entries(relabel).reduce((a, [from, to]) => a.split(from).join(to), svg));
}

// ---------------------------------------------------------------------------------------------
// The results, read from the committed files.

const noise = json<{ revertedNotVandalism: Wilson; labeller: string }>('results/phase0/label-noise.json')!;

const sealed = loadSet('sealed');
const runDir = 'results/phase5/runs/sealed';
const scores = readdirSync(runDir).filter((f) => f.endsWith('.jsonl')).map((f) => readRun(`${runDir}/${f}`)).filter((r) => r.length).map((rows) => scoreRun(sealed, rows, rows[0]!.config));
const baseRate = sealed.filter((e) => e.label === 'reverted').length / sealed.length;
const incomplete = scores.filter((x) => !x.complete).map((x) => x.config);
const calib = json<{ configs: Record<string, { overall: { ece: number; auroc: number } }> }>('results/phase6/sealed/calibration.json');

/** Plain names: "ollama:gemma3:4b__p2-guide" -> the model, without its prompt. */
const modelOf = (config: string) => config.replace(/__p\d-[\w-]+$/, '');
const PLAIN: Record<string, { name: string; kind: string }> = {
  'baseline:filter-only': { name: 'Flag every edit that gets past the free filter', kind: 'rule' },
  'baseline:filter+temporary': { name: 'Simple rule: flag logged-out editors', kind: 'rule' },
  'baseline:liftwing-revertrisk-language-agnostic': { name: "Wikipedia's own model (revert risk)", kind: 'Wikipedia' },
  'baseline:liftwing-enwiki-damaging': { name: "Wikipedia's older model (ORES)", kind: 'Wikipedia' },
  'ollama:gemma3:4b': { name: 'Small AI on a laptop (Gemma 3)', kind: 'free AI' },
  'gemini:gemini-3.5-flash-lite': { name: 'Gemini 3.5 Flash-Lite', kind: 'paid AI' },
  'gemini:gemini-3.8-flash': { name: 'Gemini 3.8 Flash', kind: 'paid AI' },
};
const plain = (config: string) => PLAIN[modelOf(config)]?.name ?? config;
// Each model with its best prompt on the sealed set (by F1), as in the README table.
const best = new Map<string, Score>();
for (const s of scores.filter((x) => x.complete)) {
  const m = modelOf(s.config);
  if (!best.has(m) || s.f1 > best.get(m)!.f1) best.set(m, s);
}
const order = Object.keys(PLAIN).filter((m) => best.has(m) && m !== 'baseline:filter-only');
const bestOf = order.map((m) => best.get(m)!);
// The best paid AI by precision: the headline compares it with the free one.
const top = bestOf.filter((s) => PLAIN[modelOf(s.config)]!.kind === 'paid AI').sort((a, b) => b.precision.rate - a.precision.rate)[0]!;
const ores = best.get('baseline:liftwing-enwiki-damaging');
const rule = best.get('baseline:filter+temporary');
const local = best.get('ollama:gemma3:4b');

const streamFilter = json<{ tables: Record<string, Record<string, { volumeRemoved: number; volumeRemovedNonBot: number; revertedLost: Wilson }>> }>('results/phase3/filter-eval-stream-labels.json')!;
const policy = Object.values(streamFilter.tables).map((t) => t.extendedconfirmed!);
const removed = { lo: Math.min(...policy.map((p) => p.volumeRemoved)), hi: Math.max(...policy.map((p) => p.volumeRemoved)) };
const lost = { lo: Math.min(...policy.map((p) => p.revertedLost.rate)), hi: Math.max(...policy.map((p) => p.revertedLost.rate)) };

type LadderPolicy = { policy: string; precision: Wilson; recall: Wilson; f1: number; escalated: number; usdPer1000ClassifiableEdits: number };
const ladder = json<{ prices: { retrievedAt: string }; ladders: { cloudModel: string; chosenOnDev: { band: { lo: number; hi: number } }; sealed: LadderPolicy[] }[] }>('results/phase7/ladder.json');
const ladder0 = ladder?.ladders[0];
const cloudOnly = ladder0?.sealed.find((p) => p.policy === 'cloud only');

const bp = existsSync('results/phase2') ? readdirSync('results/phase2').filter((f) => /^backpressure-.*\.json$/.test(f)).sort().at(-1) : undefined;
const backpressure = bp ? json<{ peakLag: number; slowMinutes: number; slowTier: { ratePerMinute: number }; verification: { handled: number; ok: boolean } }>(`results/phase2/${bp}`) : undefined;
// Plain labels for a reader outside the field; the chart itself is unchanged.
if (bp) themeSvg(`results/phase2/${bp.replace('.json', '.svg')}`, `${OUT}/img/backpressure.svg`, { 'Consumer lag: a 10/min model tier behind the feed, then draining': 'Edits waiting for a slow AI, then catching up', 'rate-limited (10 kept edits/min)': 'AI limited to 10 edits a minute', 'drain at full speed': 'AI at full speed', 'messages waiting': 'edits waiting' });

const calibImgs: string[] = [];
if (existsSync('results/phase6/sealed')) {
  for (const f of readdirSync('results/phase6/sealed').filter((f) => f.endsWith('.svg')).sort()) {
    themeSvg(`results/phase6/sealed/${f}`, `${OUT}/img/calib-${f}`);
    calibImgs.push(f);
  }
}
const eceRange = (prefix: string) => {
  const v = Object.entries(calib?.configs ?? {}).filter(([k]) => k.startsWith(prefix)).map(([, c]) => c.overall.ece);
  return v.length ? { lo: Math.min(...v), hi: Math.max(...v) } : undefined;
};
const eceLocal = eceRange('ollama:');
const eceCloud = eceRange('gemini:');

// Phase 8: the latest autoscaling report (interrupted recordings are never reported).
const p8 = existsSync('results/phase8') ? readdirSync('results/phase8').filter((f) => /^diurnal-.*\.report\.json$/.test(f)).sort().at(-1) : undefined;
type P8Hour = { start: string; feed: number; replicas: number; kept?: number };
const phase8 = p8 ? json<{ from: string; to: string; hoursSpanned: number; feedVsReplicas30min: number; keptVsReplicas30min?: number; replicasRange: { min: number; max: number }; gaps: { minutes: number }[]; quietestKeptHour?: P8Hour; busiestKeptHour?: P8Hour }>(`results/phase8/${p8}`) : undefined;
if (p8) themeSvg(`results/phase8/${p8.replace('.report.json', '.svg')}`, `${OUT}/img/phase8.svg`);

const live = json<{ labels: { total: number; revertRate: Wilson }; filter: { volumeRemoved: number; revertedLost: Wilson } }>('results/phase4/live-labels.json');
const drift = existsSync('results/phase9/alerts.jsonl') ? readFileSync('results/phase9/alerts.jsonl', 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { at: string; degradedTest: number | null; config: string; alerts: string[] }) : [];
const degraded = json<{ checks: { agreement: number; edits: number }[] }>('results/phase9/drift-2026-10-04T0640Z-degraded.json')?.checks[0];
const thresholds = json<{ minAgreement: number; basis: Record<string, { vsOriginal: number[] }> }>('results/phase9/thresholds.json');
const agreement = Object.entries(thresholds?.basis ?? {}).map(([k, b]) => ({ config: k, lowest: Math.min(...b.vsOriginal) })).sort((a, b) => a.lowest - b.lowest);
const mostShifted = agreement[0];
const others = mostShifted ? agreement.filter((a) => modelOf(a.config) !== modelOf(mostShifted.config)).map((a) => a.lowest) : [];

// ---------------------------------------------------------------------------------------------
// Pieces of the page.

/** A horizontal bar chart in HTML: one series, value labels at the bar ends, a hover tip per bar. */
function bars(rows: { label: string; sub: string; value: number; tip: string }[], max: number, ref?: { value: number; label: string }): string {
  return `<div class="bars${ref ? ' has-ref' : ''}">${ref ? `<div class="ref" style="--f:${ref.value / max}"><span>${esc(ref.label)}</span></div>` : ''}${rows
    .map((r) => `<div class="bar" tabindex="0" data-tip="${esc(r.tip)}"><div class="bar-label">${esc(r.label)}<small>${esc(r.sub)}</small></div><div class="track"><div class="fill" style="width:${(r.value / max) * 100}%"></div><span class="val">${pct(r.value)}</span></div></div>`)
    .join('')}</div>`;
}
const details = (summary: string, body: string) => `<details><summary>${esc(summary)}</summary><div class="details-body">${body}</div></details>`;
const SECTION_IDS: Record<string, string> = { 'Most undone edits aren’t vandalism': 'findings', 'Which approach is the best judge?': 'models', 'The money question': 'cost', 'Adding help when it gets busy': 'scaling', 'Noticing when an AI quietly changes': 'drift' };
const section = (n: number, title: string, lede: string, body: string, id = SECTION_IDS[title] ?? `s${n}`) => `<section id="${id}"><div class="eyebrow">${String(n).padStart(2, '0')}</div><h2>${title}</h2><p class="lede">${lede}</p>${body}</section>`;

const gridTable = `<div class="scroll"><table><tr><th>Approach (and prompt)</th><th>Right when it flags</th><th>Undone edits caught</th><th>Edits flagged</th><th>Ranking skill (AUROC)</th><th>Confidence gap (ECE)</th></tr>${[...scores]
  .sort((a, b) => a.config.localeCompare(b.config))
  .map((s) => {
    const cal = calib?.configs[s.config]?.overall;
    return s.complete
      ? `<tr><td>${esc(plain(s.config))}<br><span class="ci">${esc(s.config)}</span></td><td>${ci(s.precision)}</td><td>${ci(s.recall)}</td><td>${pct(s.flagged)}</td><td>${cal ? cal.auroc.toFixed(2) : '—'}</td><td>${cal ? cal.ece.toFixed(2) : '—'}</td></tr>`
      : `<tr class="pending"><td>${esc(s.config)}</td><td colspan="5">unfinished — ${s.edits} of ${sealed.length} edits answered; not counted</td></tr>`;
  })
  .join('')}</table></div><p class="note">1,000 test edits that got past the free filter, frozen before any model saw them. Ranges are 95% confidence intervals. AUROC: how well the scores rank risky edits above safe ones (0.5 is a coin flip, 1 is perfect). ECE: the average gap between how sure a model says it is and how often it is right (lower is better).</p>`;

const filterTable = `<div class="scroll"><table><tr><th>Hour of edits</th><th>What is skipped</th><th>Edits skipped</th><th>Undone edits lost</th></tr>${Object.entries(streamFilter.tables)
  .flatMap(([hour, pols]) => Object.entries(pols).map(([pol, s]) => `<tr><td>${esc(hourLabel(hour))}</td><td>${esc({ bots: 'Bots only', autoconfirmed: 'Bots + accounts 4+ days old with 10+ edits', extendedconfirmed: 'Bots + accounts 30+ days old with 500+ edits (used)' }[pol] ?? pol)}</td><td>${pct(s.volumeRemoved, 1)}</td><td>${ci(s.revertedLost)}</td></tr>`))
  .join('')}</table></div><p class="note">"Undone" means reverted within 72 hours, read from Wikipedia's own revert tags.</p>`;

const LADDER_PLAIN: Record<string, { name: string; how: string }> = {
  'heuristics only': { name: 'Free filter only', how: 'Flag everything that gets past it' },
  'local only': { name: 'Free AI only', how: 'The laptop model decides alone' },
  'cloud only': { name: 'Paid AI only', how: 'Gemini 3.8 Flash decides every edit' },
};
const ladderCards = ladder0
  ? `<div class="cards four">${ladder0.sealed
      .map((p) => {
        const named = LADDER_PLAIN[p.policy] ?? { name: 'The ladder', how: `Ask the paid AI only when the free one is unsure (${pct(p.escalated, 1)} of edits)` };
        const isBest = p === cloudOnly;
        return `<div class="card${isBest ? ' best' : ''}"><div class="card-k">${esc(named.name)}</div><div class="card-sub">${esc(named.how)}</div><div class="kv"><span>Right when it flags</span><b>${pct(p.precision.rate)}</b></div><div class="kv"><span>Undone edits caught</span><b>${pct(p.recall.rate)}</b></div><div class="kv"><span>Cost per 1,000 edits</span><b class="accent">$${p.usdPer1000ClassifiableEdits.toFixed(2)}</b></div></div>`;
      })
      .join('')}</div>`
  : '';
const ladderTable = ladder0
  ? `<div class="scroll"><table><tr><th>Approach</th><th>Right when it flags</th><th>Undone edits caught</th><th>F1</th><th>Sent to the paid AI</th><th>$ per 1,000 edits</th></tr>${ladder0.sealed.map((r) => `<tr><td>${esc(r.policy)}</td><td>${ci(r.precision)}</td><td>${ci(r.recall)}</td><td>${r.f1.toFixed(3)}</td><td>${pct(r.escalated, 1)}</td><td>$${r.usdPer1000ClassifiableEdits.toFixed(3)}</td></tr>`).join('')}</table></div><p class="note">The ladder's rule (send an edit up when the free AI's score is between ${ladder0.chosenOnDev.band.lo} and ${ladder0.chosenOnDev.band.hi}) was chosen on 200 separate practice edits and written down before the final test. Costs use published list prices retrieved ${esc(ladder!.prices.retrievedAt)}.</p>`
  : '';

const kept = phase8 && phase8.keptVsReplicas30min !== undefined && phase8.quietestKeptHour && phase8.busiestKeptHour;
const p8Body = phase8
  ? `<div class="chips"><div class="chip"><b>${phase8.hoursSpanned.toFixed(1)} h</b><span>recorded${phase8.gaps.length ? `, ${phase8.gaps.reduce((a, g) => a + g.minutes, 0)} min missing` : ', no gaps'}</span></div><div class="chip"><b>${phase8.replicasRange.min}–${phase8.replicasRange.max}</b><span>copies of the AI worker</span></div>${kept ? `<div class="chip"><b class="accent">${phase8.keptVsReplicas30min!.toFixed(2)}</b><span>how closely they followed the work (1 = perfectly)</span></div>` : ''}</div>
${phase8.hoursSpanned < 24 ? `<p class="flag">Partial: ${phase8.hoursSpanned.toFixed(1)} of the planned 24 hours.</p>` : ''}
<figure><img src="img/phase8.svg" alt="Edits needing the AI, number of AI workers, and edits waiting, over the day"><figcaption><b>How to read it:</b> top, how many edits needed the AI each minute; middle, how many copies of the AI worker were running; bottom, how many edits were waiting in line. As the top line rises and falls through the day, the middle one follows.</figcaption></figure>
${details('Show the details', `<p>${esc(phase8.from.slice(0, 16).replace('T', ' '))} to ${esc(phase8.to.slice(0, 16).replace('T', ' '))} UTC, on a laptop Kubernetes cluster fed live from Wikipedia. ${kept ? `Busiest hour: ${Math.round(phase8.busiestKeptHour!.kept!)} edits a minute needed the AI, with ${phase8.busiestKeptHour!.replicas.toFixed(1)} workers on average; quietest: ${Math.round(phase8.quietestKeptHour!.kept!)}, with ${phase8.quietestKeptHour!.replicas.toFixed(1)}. Correlation with the edits needing the AI, over 30-minute windows: r = ${phase8.keptVsReplicas30min}. Against all edits, including those the free filter skips: r = ${phase8.feedVsReplicas30min} — that was the measure planned first; the second was chosen after seeing it, and both are reported.` : `Correlation with the feed over 30-minute windows: r = ${phase8.feedVsReplicas30min}.`} Each worker is limited to 5 AI calls a minute, standing in for a paid service's rate limit.</p>`)}`
  : '';

const driftBody = `<div class="cards two">
  ${degraded ? `<div class="card"><div class="card-k">A deliberately broken AI</div><div class="big red">${pct(degraded.agreement)}</div><div class="card-sub">same answers as before (${Math.round(degraded.agreement * degraded.edits)} of ${degraded.edits}). The alarm sounds below ${pct(thresholds?.minAgreement ?? 0.9)} — and it did, with nobody watching.</div></div>` : ''}
  ${mostShifted && mostShifted.lowest < 1 ? `<div class="card"><div class="card-k">${esc(plain(mostShifted.config))}</div><div class="big amber">${pct(mostShifted.lowest)}</div><div class="card-sub">same answers a few hours later — a small, real change on the provider's side, inside the alarm limit.${others.length ? ` Every other AI stayed at ${pct(Math.min(...others))}–${pct(Math.max(...others))}.` : ''}</div></div>` : ''}
</div>
${details('Show the details', `<p>Each AI is re-asked 50 fixed test edits and compared with its own earlier answers; the checks repeat configurations already scored and never tune them. Alerts so far:</p>${drift.length ? `<div class="scroll"><table><tr><th>When (UTC)</th><th>AI</th><th>Alert</th></tr>${drift.slice(-10).reverse().map((d) => `<tr><td>${esc(d.at.slice(0, 16).replace('T', ' '))}</td><td>${esc(d.config)}${d.degradedTest ? ` <span class="ci">(deliberately broken: ${d.degradedTest}% of answers flipped)</span>` : ''}</td><td>${esc(d.alerts.join('; '))}</td></tr>`).join('')}</table></div>` : '<p>No alerts.</p>'}<p class="note">The check is written to run nightly as a scheduled job; so far it has been run by hand.</p>`)}`;

// ---------------------------------------------------------------------------------------------

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WikiPatrol results</title>
<meta name="description" content="Can an expensive AI spot Wikipedia edits that will be undone — and is it worth paying for? The results, in plain language.">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root { color-scheme: dark; --bg: ${C.bg}; --card: ${C.card}; --line: ${C.line}; --ink: ${C.ink}; --ink-2: ${C.ink2}; --accent: ${C.accent}; --mark: ${C.mark}; --green: ${C.green}; --red: ${C.red}; --amber: ${C.amber};
    --sans: Inter, system-ui, -apple-system, sans-serif; --head: Inter, system-ui, sans-serif; --display: 'Bebas Neue', Impact, sans-serif;
    --grad: linear-gradient(135deg, #d65cf5 0%, #8b4dff 50%, #4639ff 100%); --grad-x: linear-gradient(90deg, #d65cf5, #8b4dff 55%, #4639ff); --mono: 'JetBrains Mono', ui-monospace, monospace; }
  * { box-sizing: border-box; }
  html { background: #000; scroll-behavior: smooth; scroll-padding-top: 84px; }
  body { margin: 0; background: radial-gradient(1200px 520px at 50% -120px, rgba(139, 77, 255, 0.24), transparent 70%), linear-gradient(#141414, #000 900px); color: var(--ink); font: 400 16px/1.6 var(--sans); -webkit-font-smoothing: antialiased; min-height: 100vh; }
  .nav { position: sticky; top: 0; z-index: 10; display: flex; align-items: center; gap: 28px; padding: 14px 4%; background: linear-gradient(rgba(0,0,0,.92), rgba(0,0,0,.72)); backdrop-filter: blur(8px); border-bottom: 1px solid rgba(255,255,255,.06); }
  .grad-text, .logo, h1, .eyebrow, .accent, .tool .ico { background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }
  .logo { font: 400 34px/1 var(--display); letter-spacing: .02em; text-decoration: none; white-space: nowrap; }
  .links { display: flex; gap: 4px; overflow-x: auto; scrollbar-width: none; }
  .links::-webkit-scrollbar { display: none; }
  .links a { color: #d0d0d0; text-decoration: none; font-size: 15px; padding: 8px 18px; border-radius: 999px; white-space: nowrap; transition: background .15s, color .15s; }
  .links a:hover { color: #fff; }
  .links a.on { background: rgba(255,255,255,.16); color: #fff; font-weight: 600; }
  main { max-width: 820px; margin: 0 auto; padding: 64px 16px 80px; overflow-wrap: anywhere; }
  header { text-align: center; }
  h1 { font: 400 clamp(56px, 12vw, 104px)/.9 var(--display); letter-spacing: .02em; margin: 0; padding: 0 .04em; }
  .tag { font: 500 clamp(18px, 3vw, 22px)/1.3 var(--sans); color: #fff; margin: 14px 0 0; }
  .intro { color: var(--ink-2); max-width: 580px; margin: 14px auto 0; }
  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin: 40px 0 0; padding: 22px 0; border-top: 1px solid var(--line); border-bottom: 1px solid var(--line); }
  .stat { text-align: center; padding: 0 4px; }
  .stat b { display: block; font: 400 clamp(34px, 6vw, 46px)/1 var(--display); letter-spacing: .02em; }
  .stat span { display: block; margin-top: 8px; font: 400 11px/1.4 var(--mono); color: var(--ink-2); text-transform: uppercase; letter-spacing: 0.08em; }
  .accent { color: var(--accent); } .green { color: var(--green); } .red { color: var(--red); } .amber { color: var(--amber); }
  .banner { margin: 28px 0 0; padding: 14px 18px; background: var(--card); border: 1px solid var(--line); border-radius: 12px; color: var(--ink-2); font-size: 14px; text-align: center; }
  .banner b { color: var(--ink); font-weight: 500; }
  section { margin-top: 72px; }
  .eyebrow { display: inline-block; font: 400 18px var(--display); letter-spacing: 0.08em; }
  h2 { font: 700 clamp(22px, 4vw, 28px)/1.2 var(--head); letter-spacing: -0.01em; margin: 6px 0 10px; }
  .lede { color: var(--ink-2); margin: 0 0 22px; }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: 20px; min-width: 0; transition: border-color .15s, transform .15s; }
  .card:hover { border-color: #444; }
  .card.best { border: 1px solid transparent; background: linear-gradient(var(--card), var(--card)) padding-box, var(--grad) border-box; box-shadow: 0 12px 40px rgba(139, 77, 255, .22); }
  .card.best::before { content: 'BEST JUDGE'; display: inline-block; margin-bottom: 10px; padding: 2px 8px; border-radius: 4px; background: var(--grad); color: #fff; font: 400 14px/1.4 var(--display); letter-spacing: .08em; }
  .cards { display: grid; gap: 12px; }
  .cards.two { grid-template-columns: repeat(auto-fit, minmax(min(280px, 100%), 1fr)); }
  .cards.four { grid-template-columns: repeat(auto-fit, minmax(min(170px, 100%), 1fr)); }
  .card-k { font-weight: 500; }
  .card-sub { color: var(--ink-2); font-size: 14px; margin-top: 6px; }
  .big { font: 400 56px/1 var(--display); letter-spacing: .02em; margin: 10px 0 2px; }
  .hero { display: grid; grid-template-columns: auto 1fr; gap: 24px; align-items: center; }
  .hero .big { font-size: 96px; margin: 0; }
  .kv { display: flex; justify-content: space-between; gap: 8px; font-size: 13px; color: var(--ink-2); border-top: 1px solid var(--line); padding-top: 8px; margin-top: 10px; }
  .kv b { color: var(--ink); font: 500 15px var(--mono); white-space: nowrap; flex-shrink: 0; align-self: center; }
  .meters { display: grid; gap: 18px; }
  .meter-top { display: flex; justify-content: space-between; gap: 12px; margin-bottom: 8px; }
  .meter-top b { font: 500 18px var(--mono); }
  .meter { position: relative; height: 12px; background: var(--bg); border-radius: 6px; border: 1px solid var(--line); }
  .meter i { position: absolute; top: -1px; bottom: -1px; border-radius: 6px; background: var(--mark); }
  .meter i.ok { background: var(--green); }
  .meter i.bad { background: var(--red); opacity: .85; }
  .chart-title { font-weight: 500; margin: 0 0 4px; }
  .chart-sub { color: var(--ink-2); font-size: 14px; margin: 0 0 16px; }
  .bars { position: relative; display: grid; gap: 12px; }
  .bar { display: grid; grid-template-columns: minmax(0, 210px) 1fr; gap: 14px; align-items: center; position: relative; outline: none; }
  .bar-label { font-size: 14px; line-height: 1.3; }
  .bar-label small { display: block; font: 400 11px var(--mono); color: var(--ink-2); text-transform: uppercase; letter-spacing: .06em; margin-top: 2px; }
  .track { position: relative; height: 22px; display: flex; align-items: center; }
  .fill { height: 100%; background: var(--grad-x); border-radius: 0 4px 4px 0; min-width: 2px; }
  .val { font: 500 13px var(--mono); margin-left: 8px; white-space: nowrap; }
  .bar:hover .fill, .bar:focus .fill { filter: brightness(1.2); }
  .bar[data-tip]:hover::after, .bar[data-tip]:focus::after { content: attr(data-tip); position: absolute; z-index: 2; left: 224px; top: calc(100% + 6px); max-width: 320px; background: #262626; border: 1px solid var(--line); color: var(--ink); font-size: 13px; padding: 8px 10px; border-radius: 8px; }
  .bars.has-ref { margin-top: 22px; }
  .ref { position: absolute; top: -6px; bottom: -6px; left: calc(224px + (100% - 224px) * var(--f)); pointer-events: none; }
  .ref::before { content: ''; position: absolute; top: 0; bottom: 0; border-left: 1px dashed var(--ink-2); }
  .ref span { position: absolute; bottom: 100%; transform: translateX(-50%); white-space: nowrap; font: 400 11px var(--mono); color: var(--ink-2); }
  .chips { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(160px, 100%), 1fr)); gap: 12px; margin-bottom: 16px; }
  .chip { background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 14px; }
  .chip b { display: block; font: 400 34px/1.1 var(--display); letter-spacing: .02em; }
  .chip span { color: var(--ink-2); font-size: 13px; }
  .flag { color: var(--amber); font-size: 14px; margin: 0 0 12px; }
  figure { margin: 0; }
  img { display: block; max-width: 100%; height: auto; border-radius: 12px; border: 1px solid var(--line); background: var(--card); }
  figcaption { color: var(--ink-2); font-size: 14px; margin-top: 10px; }
  figcaption b { color: var(--ink); font-weight: 500; }
  .figs { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(220px, 100%), 1fr)); gap: 10px; }
  details { margin-top: 16px; border-top: 1px solid var(--line); }
  summary { cursor: pointer; padding: 14px 0 0; font: 400 13px var(--mono); color: var(--ink-2); list-style: none; }
  summary::-webkit-details-marker { display: none; }
  summary::before { content: '+ '; color: var(--accent); }
  details[open] summary::before { content: '− '; }
  summary:hover { color: var(--ink); }
  .details-body { padding-top: 12px; font-size: 14px; color: var(--ink-2); }
  .scroll { overflow-x: auto; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { text-align: left; padding: 8px; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { color: var(--ink-2); font: 500 11px var(--mono); text-transform: uppercase; letter-spacing: .06em; }
  td { color: var(--ink); }
  .ci { color: var(--ink-2); font-size: 11px; font-family: var(--mono); }
  tr.pending td { color: var(--ink-2); font-style: italic; }
  .note { font-size: 13px; color: var(--ink-2); }
  a { color: var(--accent); text-underline-offset: 3px; }
  footer { margin-top: 80px; padding-top: 24px; border-top: 1px solid var(--line); color: var(--ink-2); font-size: 13px; text-align: center; }
  .stack { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(300px, 100%), 1fr)); gap: 12px; }
  .tool { display: grid; grid-template-columns: 40px 1fr; gap: 14px; align-items: start; }
  .tool .ico { width: 40px; height: 40px; border-radius: 6px; display: grid; place-items: center; font: 400 20px var(--display); letter-spacing: .02em; box-shadow: inset 0 0 0 1px #333; }
  .tool b { font-weight: 600; }
  .tool .role { font: 500 11px var(--mono); color: var(--accent); text-transform: uppercase; letter-spacing: .08em; margin: 2px 0 4px; }
  .tool p { margin: 0; color: var(--ink-2); font-size: 14px; }
  .flow { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin: 0 0 18px; font-size: 13px; }
  .flow span { background: var(--card); border: 1px solid var(--line); border-radius: 999px; padding: 6px 12px; }
  .flow i { color: var(--accent); font-style: normal; }
  @media (max-width: 620px) {
    .nav { gap: 14px; padding: 12px 16px; }
    .logo { font-size: 28px; }
    .links a { padding: 7px 12px; font-size: 14px; }
    .stats { grid-template-columns: repeat(2, 1fr); row-gap: 22px; }
    .hero { grid-template-columns: 1fr; }
    .bar { grid-template-columns: 1fr; gap: 6px; }
    .ref { display: none; }
    .bar[data-tip]:hover::after, .bar[data-tip]:focus::after { left: 0; }
  }
</style>
</head>
<body id="top">
<nav class="nav" aria-label="Sections">
  <a class="logo" href="#top">WikiPatrol</a>
  <div class="links">
    <a href="#top" class="on">Home</a>
    <a href="#findings">Findings</a>
    <a href="#models">AI models</a>
    <a href="#cost">Cost</a>
    ${phase8 ? '<a href="#scaling">Scaling</a>' : ''}
    <a href="#drift">Drift</a>
    <a href="#stack">Tech stack</a>
  </div>
</nav>
<main>

<header id="home">
  <h1>WikiPatrol</h1>
  <p class="tag">is an expensive AI worth paying for?</p>
  <p class="intro">Anyone can edit Wikipedia, and some edits get undone. We tested whether AI can spot those edits as they happen, and whether a paid AI is worth its cost compared with a free one.</p>
  <div class="stats">
    <div class="stat"><b class="amber">${pct(noise.revertedNotVandalism.rate)}</b><span>of undone edits weren't vandalism</span></div>
    <div class="stat"><b class="green">${pct(removed.lo)}+</b><span>of edits skipped for free</span></div>
    <div class="stat"><b>${pct(top.precision.rate)}</b><span>right when the paid AI flags one</span></div>
    <div class="stat"><b class="accent">$${(cloudOnly?.usdPer1000ClassifiableEdits ?? 0).toFixed(2)}</b><span>per 1,000 edits for the paid AI</span></div>
  </div>
  <div class="banner"><b>Recorded results, not a live feed.</b> Every number comes from real Wikipedia edits, recorded and frozen before the AIs saw them. Built from the project's files on ${new Date().toISOString().slice(0, 10)}.</div>
</header>

${section(1, 'Most undone edits aren’t vandalism', 'We expected undone edits to be mostly vandalism. They aren’t — most are honest edits caught up in a disagreement or a clean-up. So this project predicts “will this edit be undone?”, not “is this vandalism?”.', `
<div class="card hero"><div class="big amber">${pct(noise.revertedNotVandalism.rate)}</div><div><div class="card-k">of undone edits were honest edits, not vandalism</div><div class="card-sub">Checked on a sample of 100 edits. Likely between ${pct(noise.revertedNotVandalism.low)} and ${pct(noise.revertedNotVandalism.high)}.</div></div></div>
${details('Show the details', `<p>50 undone and 50 kept edits, sorted into vandalism or not by ${esc(noise.labeller)}. None of the kept edits were vandalism. "Undone" means reverted within 72 hours.</p>`)}`)}

${section(2, 'A free first step does a lot of the work', 'Before any AI, we skip edits from bots and from experienced editors (accounts at least 30 days old with 500+ edits). It costs nothing and removes most of the work — but it lets a few edits that later get undone slip past.', `
<div class="card meters">
  <div><div class="meter-top"><span>Edits skipped</span><b class="green">${pct(removed.lo)}–${pct(removed.hi)}</b></div><div class="meter"><i class="ok" style="left:${removed.lo * 100}%;width:${(removed.hi - removed.lo) * 100}%"></i><i class="ok" style="left:0;width:${removed.lo * 100}%;opacity:.45"></i></div></div>
  <div><div class="meter-top"><span>Undone edits it lets slip</span><b class="red">${pct(lost.lo)}–${pct(lost.hi)}</b></div><div class="meter"><i class="bad" style="left:${lost.lo * 100}%;width:${(lost.hi - lost.lo) * 100}%"></i><i class="bad" style="left:0;width:${lost.lo * 100}%;opacity:.45"></i></div></div>
  <div class="card-sub">Ranges cover three separate hours of edits. The edits that slip past look like honest mistakes, not vandalism.</div>
</div>
${details('Show the details', filterTable)}`)}

${section(3, 'Which approach is the best judge?', `Every approach looked at the same ${sealed.length.toLocaleString('en-US')} edits. Of these, ${pct(baseRate)} were later undone — so flagging edits at random would be right about ${pct(baseRate)} of the time.`, `
<div class="card">
  <p class="chart-title">When it says “this will be undone”, how often is it right?</p>
  <p class="chart-sub">Higher is better. The dashed line is a random guess.</p>
  ${bars(bestOf.map((s) => ({ label: plain(s.config), sub: PLAIN[modelOf(s.config)]!.kind, value: s.precision.rate, tip: `Right ${pct(s.precision.rate)} of the time when it flags an edit (likely ${pct(s.precision.low)}–${pct(s.precision.high)}).` })), 0.5, { value: baseRate, label: `random ${pct(baseRate)}` })}
</div>
<div class="card" style="margin-top:12px">
  <p class="chart-title">Out of all the edits that got undone, how many did it catch?</p>
  <p class="chart-sub">Higher is better — but an approach can catch more by flagging almost everything.</p>
  ${bars(bestOf.map((s) => ({ label: plain(s.config), sub: PLAIN[modelOf(s.config)]!.kind, value: s.recall.rate, tip: `Caught ${pct(s.recall.rate)} of undone edits, flagging ${pct(s.flagged)} of all edits.` })), 1)}
</div>
<p class="note">The paid AI (${esc(plain(top.config))}) is right ${pct(top.precision.rate)} of the time when it flags an edit${local ? `, against ${pct(local.precision.rate)} for the free laptop AI` : ''}; the free AI catches more undone edits only because it flags most of everything.${ores ? ` Wikipedia’s older model is right even more often (${pct(ores.precision.rate)}) but flags so few edits that it catches only ${pct(ores.recall.rate)}.` : ''}${rule ? ` And a one-line rule — flag logged-out editors — is right ${pct(rule.precision.rate)} of the time, not far behind.` : ''}</p>
${details('Show all approaches and prompts', gridTable)}`)}

${eceLocal && eceCloud ? section(4, 'Does the AI know when it’s guessing?', 'Each AI also says how sure it is. That only helps if “90% sure” really means right 9 times in 10.', `
<div class="cards two">
  <div class="card"><div class="card-k">Paid AIs (Gemini)</div><div class="big green">Partly</div><div class="card-sub">Their confidence is off by ${pct(eceCloud.lo)}–${pct(eceCloud.hi)} on average — but when they’re more sure, they tend to be more right.</div></div>
  <div class="card"><div class="card-k">Free laptop AI (Gemma 3)</div><div class="big red">No</div><div class="card-sub">Its confidence is off by ${pct(eceLocal.lo)}–${pct(eceLocal.hi)} on average. It sounds very sure even when it’s wrong.</div></div>
</div>
${calibImgs.length ? details('Show the confidence charts', `<p>Each chart compares how sure a model said it was (across) with how often those edits were really undone (up). Points on the diagonal mean its confidence can be trusted.</p><div class="figs">${calibImgs.map((f) => `<img src="img/calib-${f}" alt="Confidence chart: ${esc(f.replace('.svg', ''))}" loading="lazy">`).join('')}</div>`) : ''}`) : ''}

${ladder0 ? section(5, 'The money question', 'The plan: let the free AI handle everything, and ask the paid one only when the free one is unsure. It didn’t work — the free AI never knows when it’s unsure (see above), so it almost never asked for help.', `
${ladderCards}
<p class="note">At English Wikipedia’s volume, asking the paid AI about every edit that passes the free step would cost roughly $17–28 a day at published prices.</p>
${details('Show the details', ladderTable)}`) : ''}

${phase8 ? section(6, 'Adding help when it gets busy', 'The AI step runs as several copies of a worker. When edits pile up, more copies start; when it’s quiet, some stop. Over a full day of live Wikipedia edits, the number of copies followed the real workload.', p8Body) : ''}

${backpressure ? section(7, 'A waiting room for edits', 'A slow, rate-limited AI can’t keep up with the edit stream, so edits wait in line until it gets to them. Nothing is lost while they wait.', `
<figure><img src="img/backpressure.svg" alt="Edits waiting over time"><figcaption><b>How to read it:</b> the line is how many edits were waiting. With the AI limited to ${backpressure.slowTier.ratePerMinute} edits a minute, ${backpressure.peakLag.toLocaleString('en-US')} piled up in ${backpressure.slowMinutes} minutes. At full speed the line drains back to zero — and all ${backpressure.verification.handled.toLocaleString('en-US')} edits were handled ${backpressure.verification.ok ? 'exactly once' : '(check failed)'}.</figcaption></figure>`) : ''}

${section(phase8 ? 8 : 6, 'Noticing when an AI quietly changes', 'AI companies sometimes change a model without changing its name. We re-ask each AI the same 50 questions and compare its answers with last time.', driftBody)}

${live ? section(9, 'Labels from the live system', 'Edits the live system recorded, checked 72 hours later.', `<div class="cards two"><div class="card"><div class="big">${live.labels.total.toLocaleString('en-US')}</div><div class="card-sub">edits labelled automatically; ${pct(live.labels.revertRate.rate, 1)} were undone.</div></div><div class="card"><div class="big">${pct(live.filter.volumeRemoved)}</div><div class="card-sub">skipped by the free step, letting ${pct(live.filter.revertedLost.rate)} of undone edits slip.</div></div></div>`) : ''}

<section id="stack"><div class="eyebrow">STACK</div><h2>How it’s built</h2><p class="lede">The tools behind the results, and what each one does — in a sentence.</p>
<div class="flow"><span>Wikipedia’s live edits</span><i>→</i><span>Recorded in a queue</span><i>→</i><span>Free filter</span><i>→</i><span>AI judges the rest</span><i>→</i><span>Results on this page</span></div>
<div class="stack">
${[
  ['W', 'Wikipedia’s public feeds', 'The data', 'Every edit as it happens, which edits were later undone, and details about each editor. Free, no login.'],
  ['TS', 'TypeScript and Node.js', 'The code', 'The language every part of the project is written in.'],
  ['K', 'Kafka (Redpanda)', 'The waiting room', 'A queue that holds edits in order until the next step is ready, and keeps a record so tests can be replayed exactly.'],
  ['D', 'Docker', 'The packaging', 'Bundles the code and everything it needs into one box that runs the same anywhere.'],
  ['K8', 'Kubernetes (kind) + KEDA', 'The manager', 'Runs the workers, restarts any that get stuck, and adds more AI workers when edits pile up.'],
  ['AI', 'Ollama with Gemma 3', 'The free AI', 'A small AI model running on a laptop, at no cost per question.'],
  ['G', 'Google Gemini', 'The paid AI', 'Cloud AI models that charge per question; the costs here use their published prices.'],
  ['WM', 'Wikimedia LiftWing', 'The benchmark', 'Wikipedia’s own prediction models, used as the bar to compare against.'],
  ['CI', 'Vitest + GitHub Actions', 'The safety net', 'Tests that run on every change, including spinning up the whole system on a throwaway cluster.'],
  ['V', 'Vercel', 'This page', 'Hosts this results page.'],
].map(([ico, name, role, what]) => `<div class="card tool"><div class="ico">${ico}</div><div><b>${esc(name!)}</b><div class="role">${esc(role!)}</div><p>${esc(what!)}</p></div></div>`).join('\n')}
</div>
</section>

<footer>
  <p>Code, every decision and what went wrong: <a href="https://github.com/spoigai21/WikiPatrol">github.com/spoigai21/WikiPatrol</a></p>
  <p>Costs come from dated published prices, not from the free tiers used for most runs.${incomplete.length ? ` Not counted (unfinished on a free tier): ${incomplete.map(esc).join(', ')}.` : ''}</p>
</footer>

</main>
<script>
  // Highlight the section in view in the top bar.
  const links = [...document.querySelectorAll('.links a')];
  const targets = links.map((a) => document.querySelector(a.getAttribute('href'))).filter(Boolean);
  const mark = () => {
    const y = window.scrollY + 120;
    let current = links[0];
    targets.forEach((t, i) => { if (t.offsetTop <= y && t.id !== 'top') current = links[i]; });
    if (window.scrollY > 0 && window.innerHeight + window.scrollY >= document.body.scrollHeight - 4) current = links.at(-1);
    links.forEach((a) => a.classList.toggle('on', a === current));
  };
  addEventListener('scroll', mark, { passive: true });
  mark();
</script>
</body>
</html>
`;
writeFileSync(`${OUT}/index.html`, html);
log(`wrote ${OUT}/index.html (${scores.length} configurations, ${calibImgs.length} confidence charts)`);
