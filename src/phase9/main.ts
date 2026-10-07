// Phase 9: the nightly drift check (D15). Run by a Kubernetes CronJob, or by hand.
//
//   npm run phase9:drift                                  check every eligible configuration
//   npm run phase9:drift -- --measure-variance            run the subset twice; set the threshold
//   npm run phase9:drift -- --configs ollama:gemma3:4b__p1-plain --degrade 30   prove the alert fires
//
// Exit code 1 when any configuration trips an alert (a failed Job is the alert's first channel).

import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync } from 'node:fs';
import { args, log, stamp, writeJson } from '../phase0/cli.ts';
import { round } from '../phase0/stats.ts';
import { clientFor } from '../models/clients.ts';
import { baselineFor, LlmPredictor, type Predictor } from '../phase5/predictors.ts';
import { PROMPT_IDS, type PromptId } from '../phase5/prompts.ts';
import { loadSet, readRun, runPath, runPredictor, SEALED, type RunRow } from '../phase5/runner.ts';
import { chooseSubset, compare, DEFAULT_THRESHOLDS, degrade, type DriftCheck, type DriftThresholds } from './drift.ts';

const opts = args({
  configs: { type: 'string' },
  degrade: { type: 'string' },
  'measure-variance': { type: 'boolean', default: false },
  rpm: { type: 'string', default: '10' },
});

const DIR = 'results/phase9';
const SUBSET = `${DIR}/subset.json`;
const THRESHOLDS = `${DIR}/thresholds.json`;
mkdirSync(DIR, { recursive: true });

const sealed = loadSet(SEALED);
if (!existsSync(SUBSET)) {
  writeJson(SUBSET, { chosenAt: new Date().toISOString(), seed: 20261004, note: 'Fixed drift subset of the sealed set (D15). Never re-drawn.', revIds: chooseSubset(sealed) });
  log(`wrote ${SUBSET}`);
}
const subsetIds = new Set((JSON.parse(readFileSync(SUBSET, 'utf8')) as { revIds: number[] }).revIds);
const subset = sealed.filter((e) => subsetIds.has(e.revId));

/** Configurations with a complete sealed run that a rerun can repeat (rules cannot drift). */
function eligible(): string[] {
  const dir = `results/phase5/runs/${SEALED}`;
  return readdirSync(dir)
    .filter((f) => f.endsWith('.jsonl'))
    .map((f) => readRun(`${dir}/${f}`))
    .filter((rows) => rows.length >= sealed.length)
    .map((rows) => rows[0]!.config)
    .filter((c) => !c.startsWith('baseline:filter'));
}

function predictorFor(config: string): Predictor {
  if (config.startsWith('baseline:')) return baselineFor(config.slice('baseline:'.length));
  const [model, prompt] = config.split('__') as [string, PromptId];
  if (!(PROMPT_IDS as readonly string[]).includes(prompt)) throw new Error(`cannot rebuild ${config}`);
  return new LlmPredictor(clientFor(model, model.startsWith('ollama:') ? undefined : Number(opts.rpm)), prompt);
}

async function rerun(config: string, label: string): Promise<{ check: DriftCheck; original: RunRow[] }> {
  const base = predictorFor(config);
  const predictor = opts.degrade ? degrade(base, Number(opts.degrade)) : base;
  const out = `${DIR}/runs/${label}/${config.replace(/[^\w.+-]+/g, '_')}.jsonl`;
  const r = await runPredictor({ set: 'drift', edits: subset, predictor, out, log: (m) => log(`[${config}] ${m}`) });
  if (r.stopped) throw new Error(`${config}: rerun stopped (${r.stopped}); not a drift result`);
  const original = readRun(runPath(SEALED, config)).filter((x) => subsetIds.has(x.revId));
  return { check: compare(config, original, readRun(out), thresholds()), original };
}

function thresholds(): DriftThresholds {
  return existsSync(THRESHOLDS) ? { ...DEFAULT_THRESHOLDS, ...(JSON.parse(readFileSync(THRESHOLDS, 'utf8')) as Partial<DriftThresholds>) } : DEFAULT_THRESHOLDS;
}

const configs = opts.configs ? String(opts.configs).split(',') : eligible();
const runStamp = stamp();

if (opts['measure-variance']) {
  // Two back-to-back reruns: how much does a configuration disagree with itself when nothing changed?
  const per: Record<string, { vsOriginal: number[]; runToRun: number }> = {};
  for (const config of configs) {
    const a = await rerun(config, `variance-${runStamp}-a`);
    const b = await rerun(config, `variance-${runStamp}-b`);
    const ra = readRun(`${DIR}/runs/variance-${runStamp}-a/${config.replace(/[^\w.+-]+/g, '_')}.jsonl`);
    const rb = readRun(`${DIR}/runs/variance-${runStamp}-b/${config.replace(/[^\w.+-]+/g, '_')}.jsonl`);
    per[config] = { vsOriginal: [a.check.agreement, b.check.agreement], runToRun: compare(config, ra, rb).agreement };
    log(`${config}: vs Phase 5 ${a.check.agreement} / ${b.check.agreement}; run to run ${per[config]!.runToRun}`);
  }
  // Merge with configurations measured earlier: the threshold covers every configuration checked.
  const earlier = existsSync(THRESHOLDS) ? ((JSON.parse(readFileSync(THRESHOLDS, 'utf8')) as { basis?: typeof per }).basis ?? {}) : {};
  const basis = { ...earlier, ...per };
  const lowest = Math.min(...Object.values(basis).flatMap((p) => p.vsOriginal));
  const minAgreement = round(Math.min(DEFAULT_THRESHOLDS.minAgreement, lowest - 0.05));
  writeJson(THRESHOLDS, { measuredAt: new Date().toISOString(), minAgreement, maxInvalidRise: DEFAULT_THRESHOLDS.maxInvalidRise, basis, rule: 'min(0.90, lowest agreement with Phase 5 over two reruns - 0.05), over every configuration measured (D15)' });
  log(`threshold: agreement >= ${minAgreement} -> ${THRESHOLDS}`);
  process.exit(0);
}

const checks: DriftCheck[] = [];
for (const config of configs) {
  try {
    checks.push((await rerun(config, `${opts.degrade ? 'degraded' : 'nightly'}-${runStamp}`)).check);
  } catch (err) {
    checks.push({ config, edits: 0, agreement: NaN, flips: [], meanAbsDeltaP: null, invalidBefore: 0, invalidNow: 0, versionsBefore: [], versionsNow: [], alerts: [`rerun failed: ${err instanceof Error ? err.message : String(err)}`] });
  }
}

const report = { checkedAt: new Date().toISOString(), degradedTest: opts.degrade ? Number(opts.degrade) : null, subset: subset.length, thresholds: thresholds(), checks };
writeJson(`${DIR}/drift-${runStamp}${opts.degrade ? '-degraded' : ''}.json`, report);
appendFileSync(`${DIR}/history.jsonl`, JSON.stringify({ at: report.checkedAt, degradedTest: report.degradedTest, checks: checks.map(({ flips: _, ...c }) => c) }) + '\n');

const alerts = checks.filter((c) => c.alerts.length);
for (const c of checks) log(`${c.config}: agreement ${c.agreement}${c.alerts.length ? ` — ALERT: ${c.alerts.join('; ')}` : ''}`);
if (alerts.length) {
  for (const c of alerts) appendFileSync(`${DIR}/alerts.jsonl`, JSON.stringify({ at: report.checkedAt, degradedTest: report.degradedTest, config: c.config, alerts: c.alerts }) + '\n');
  const hook = process.env.DRIFT_WEBHOOK;
  if (hook) {
    const text = `WikiPatrol drift${report.degradedTest ? ' (degraded test)' : ''}: ${alerts.map((c) => `${c.config}: ${c.alerts.join('; ')}`).join(' | ')}`;
    await fetch(hook, { method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: text }).catch((e) => log(`webhook failed: ${e}`));
  }
  process.exit(1);
}
