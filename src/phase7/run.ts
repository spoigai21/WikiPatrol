// Phase 7: choose the ladder on the dev set by the rules in DECISIONS.md D17, then score the four
// policies once on the sealed set.
//
//   npm run phase7:ladder

import { readFileSync, writeFileSync } from 'node:fs';
import { args, log, writeJson } from '../phase0/cli.ts';
import { round } from '../phase0/stats.ts';
import type { SetEdit } from '../phase5/build-set.ts';
import { PROMPT_IDS } from '../phase5/prompts.ts';
import { loadSet, readRun, runPath, type RunRow } from '../phase5/runner.ts';
import { scoreRun } from '../phase5/score.ts';
import { chooseBand, comparePolicies, loadPrices, perThousandEdits, type PolicyResult } from './ladder.ts';

const opts = args({
  local: { type: 'string', default: 'ollama:gemma3:4b' },
  clouds: { type: 'string', default: 'gemini:gemini-3.8-flash,gemini:gemini-3.5-flash-lite' },
  prices: { type: 'string', default: 'results/prices/2026-10-04.json' },
  target: { type: 'string', default: '0.95' },
});

const prices = loadPrices(String(opts.prices));
const dev = loadSet('dev');
const sealed = loadSet('sealed');
const manifest = JSON.parse(readFileSync('results/phase5/sets/sealed.manifest.json', 'utf8')) as { classifiableEdits: number; filter: { kept: number } };
const survivorShare = manifest.filter.kept / manifest.classifiableEdits;

const rows = (set: string, model: string, prompt: string) => {
  const r = readRun(runPath(set, `${model}__${prompt}`));
  const need = set === 'dev' ? dev.length : sealed.length;
  if (r.length < need) throw new Error(`${model}__${prompt} on ${set}: ${r.length}/${need} answers; the run is not complete`);
  return r;
};

/** D17: the prompt with the highest dev F1; ties to fewer tokens per edit. */
function bestPrompt(model: string) {
  const scored = PROMPT_IDS.map((p) => {
    const r = rows('dev', model, p);
    const s = scoreRun(dev, r, `${model}__${p}`);
    return { prompt: p, f1: s.f1, tokens: (s.tokensPerEdit?.in ?? 0) + (s.tokensPerEdit?.out ?? 0) };
  });
  scored.sort((a, b) => b.f1 - a.f1 || a.tokens - b.tokens);
  return { chosen: scored[0]!.prompt, devF1ByPrompt: Object.fromEntries(scored.map((s) => [s.prompt, s.f1])) };
}

const localModel = String(opts.local);
const local = bestPrompt(localModel);
log(`local ${localModel}: ${local.chosen} (dev F1 ${JSON.stringify(local.devF1ByPrompt)})`);

const fmt = (r: PolicyResult) => ({
  policy: r.policy,
  precision: r.precision,
  recall: r.recall,
  f1: r.f1,
  escalated: r.escalated,
  usdPer1000ClassifiableEdits: round(perThousandEdits(r.costPerModelEdit, survivorShare)),
  latencyMs: r.latencyMs,
});

const ladders = String(opts.clouds).split(',').map((cloudModel) => {
  const cloud = bestPrompt(cloudModel);
  const devLocal = rows('dev', localModel, local.chosen);
  const devCloud = rows('dev', cloudModel, cloud.chosen);
  const { band, result: devLadder, cloudRecall: devCloudRecall } = chooseBand(dev, devLocal, devCloud, prices, Number(opts.target));
  log(`cloud ${cloudModel}: ${cloud.chosen}; band [${band.lo}, ${band.hi}) escalates ${(devLadder.escalated * 100).toFixed(1)}% on dev`);

  // The single sealed scoring.
  const sealedResults = comparePolicies(sealed, rows('sealed', localModel, local.chosen), rows('sealed', cloudModel, cloud.chosen), band, prices).map(fmt);
  const [, , cloudOnly, ladder] = sealedResults;
  return {
    cloudModel,
    chosenOnDev: {
      localPrompt: local.chosen,
      cloudPrompt: cloud.chosen,
      cloudDevF1ByPrompt: cloud.devF1ByPrompt,
      band,
      devLadder: { recall: devLadder.recall.rate, escalated: devLadder.escalated, f1: devLadder.f1 },
      devCloudOnlyRecall: devCloudRecall,
    },
    sealed: sealedResults,
    headline: {
      recallKept: round(ladder!.recall.rate / cloudOnly!.recall.rate),
      costShare: cloudOnly!.usdPer1000ClassifiableEdits ? round(ladder!.usdPer1000ClassifiableEdits / cloudOnly!.usdPer1000ClassifiableEdits) : null,
    },
  };
});

const noise = JSON.parse(readFileSync('results/phase0/label-noise.json', 'utf8')) as Record<string, any>;
const report = {
  scoredAt: new Date().toISOString(),
  rules: 'DECISIONS.md D17',
  prices: { file: String(opts.prices), retrievedAt: prices.retrievedAt },
  population: `the ${sealed.length} sealed edits that pass the filter (D12); the filter sends ${(survivorShare * 100).toFixed(1)}% of classifiable edits to a model and itself loses 13–24% of reverted edits (D10)`,
  labelNoise: { revertedNotVandalism: noise.revertedNotVandalism, labeller: noise.labeller },
  local: { model: localModel, ...local },
  ladders,
};
writeJson('results/phase7/ladder.json', report);

const pct = (x: number) => `${(x * 100).toFixed(1)}%`;
const ci = (w: { rate: number; low: number; high: number }) => `${pct(w.rate)} [${pct(w.low)}, ${pct(w.high)}]`;
const md = [
  '# Phase 7 — the ladder (sealed set, scored once)',
  '',
  `Rules: DECISIONS.md D17. Prices: list prices retrieved ${prices.retrievedAt}. Revert prediction, not vandalism: ${pct(noise.revertedNotVandalism.rate)} of reverted edits were not vandalism (${noise.labeller}).`,
  '',
  ...ladders.flatMap((l) => [
    `## Cloud step: ${l.cloudModel} (${l.chosenOnDev.cloudPrompt}); local: ${localModel} (${l.chosenOnDev.localPrompt}); escalate when p_revert in [${l.chosenOnDev.band.lo}, ${l.chosenOnDev.band.hi})`,
    '',
    '| Policy | Precision | Recall | F1 | Sent to cloud | $ per 1,000 edits | p95 latency |',
    '|---|---|---|---|---|---|---|',
    ...l.sealed.map((r) => `| ${r.policy} | ${ci(r.precision)} | ${ci(r.recall)} | ${r.f1.toFixed(3)} | ${pct(r.escalated)} | $${r.usdPer1000ClassifiableEdits.toFixed(4)} | ${r.latencyMs.p95} ms |`),
    '',
    `**The ladder held ${pct(l.headline.recallKept)} of cloud-only recall at ${l.headline.costShare === null ? '—' : pct(l.headline.costShare)} of cloud-only cost.**`,
    '',
  ]),
];
writeFileSync('results/phase7/ladder.md', md.join('\n'));
log('wrote results/phase7/ladder.{json,md}');
