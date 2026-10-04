// Phase 7: the ladder — the economic core.
//
// Four policies over the same edits: heuristics only, local only, cloud only, and the ladder
// (filter -> local model -> cloud model only when the local model is unsure). Simulated offline
// from the Phase 5 run files: every policy's answer for an edit is one some model actually gave.
// Cost is dollars per 1,000 classifiable edits at the dated list prices (results/prices), never
// "$0 because free tier" (SPEC Phase 7).

import { readFileSync } from 'node:fs';
import { round, percentile, wilson } from '../phase0/stats.ts';
import type { SetEdit } from '../phase5/build-set.ts';
import type { RunRow } from '../phase5/runner.ts';

export interface Prices {
  retrievedAt: string;
  models: Record<string, { input: number; output: number }>;
}

export function loadPrices(path: string): Prices {
  return JSON.parse(readFileSync(path, 'utf8')) as Prices;
}

/** Dollars for one answer: tokens at the model's list price per million. */
export function answerCost(row: RunRow, prices: Prices): number {
  const model = row.config.split('__')[0]!;
  const p = prices.models[model];
  if (!p) throw new Error(`no price for ${model} in the ${prices.retrievedAt} table`);
  return ((row.tokensIn ?? 0) * p.input + (row.tokensOut ?? 0) * p.output) / 1_000_000;
}

/** Escalate when the local model's p_revert falls in [lo, hi), or when it gave no valid answer. */
export interface Band {
  lo: number;
  hi: number;
}

export interface PolicyResult {
  policy: string;
  precision: ReturnType<typeof wilson>;
  recall: ReturnType<typeof wilson>;
  f1: number;
  escalated: number;
  /** Mean model cost per edit that reaches the models (survives the filter), USD. */
  costPerModelEdit: number;
  latencyMs: { p50: number; p95: number };
}

interface Decision {
  flagged: boolean;
  cost: number;
  latencyMs: number;
  escalated: boolean;
}

function evaluate(policy: string, edits: readonly SetEdit[], decide: (e: SetEdit) => Decision): PolicyResult {
  const scored = edits.filter((e) => e.label === 'reverted' || e.label === 'not-reverted');
  let tp = 0, fp = 0, fn = 0, esc = 0, cost = 0;
  const lat: number[] = [];
  for (const e of scored) {
    const d = decide(e);
    const actual = e.label === 'reverted';
    if (d.flagged && actual) tp++;
    else if (d.flagged) fp++;
    else if (actual) fn++;
    if (d.escalated) esc++;
    cost += d.cost;
    lat.push(d.latencyMs);
  }
  lat.sort((a, b) => a - b);
  return {
    policy,
    precision: wilson(tp, tp + fp),
    recall: wilson(tp, tp + fn),
    f1: round(tp === 0 ? 0 : (2 * tp) / (2 * tp + fp + fn)),
    escalated: round(esc / scored.length),
    costPerModelEdit: cost / scored.length,
    latencyMs: { p50: percentile(lat, 50), p95: percentile(lat, 95) },
  };
}

export function comparePolicies(edits: readonly SetEdit[], local: readonly RunRow[], cloud: readonly RunRow[], band: Band, prices: Prices): PolicyResult[] {
  const L = new Map(local.map((r) => [r.revId, r]));
  const C = new Map(cloud.map((r) => [r.revId, r]));
  const need = (m: Map<number, RunRow>, e: SetEdit, what: string) => {
    const r = m.get(e.revId);
    if (!r) throw new Error(`${what} run has no answer for rev ${e.revId}`);
    return r;
  };
  const cloudOnly = (e: SetEdit): Decision => {
    const c = need(C, e, 'cloud');
    return { flagged: c.revert === true, cost: answerCost(c, prices), latencyMs: c.latencyMs, escalated: true };
  };
  const localOnly = (e: SetEdit): Decision => {
    const l = need(L, e, 'local');
    return { flagged: l.revert === true, cost: 0, latencyMs: l.latencyMs, escalated: false };
  };
  return [
    // Heuristics only: the filter passed it, so it is flagged (D12's filter-only baseline).
    evaluate('heuristics only', edits, () => ({ flagged: true, cost: 0, latencyMs: 0, escalated: false })),
    evaluate('local only', edits, localOnly),
    evaluate('cloud only', edits, cloudOnly),
    evaluate(`ladder [${band.lo}, ${band.hi})`, edits, (e) => {
      const l = need(L, e, 'local');
      const unsure = l.pRevert === null || (l.pRevert >= band.lo && l.pRevert < band.hi);
      if (!unsure) return { flagged: l.revert === true, cost: 0, latencyMs: l.latencyMs, escalated: false };
      const c = cloudOnly(e);
      return { ...c, latencyMs: l.latencyMs + c.latencyMs };
    }),
  ];
}

/**
 * The band, chosen on the dev set only, by a rule fixed before looking at any sealed ladder result:
 * the lowest escalation rate whose recall is at least `target` of cloud-only recall (default 95%),
 * ties broken by higher F1. Candidate edges are the distinct p values the local model gives.
 */
export function chooseBand(devEdits: readonly SetEdit[], local: readonly RunRow[], cloud: readonly RunRow[], prices: Prices, target = 0.95): { band: Band; result: PolicyResult; cloudRecall: number } {
  const edges = [...new Set([0, 1.000001, ...local.flatMap((r) => (r.pRevert === null ? [] : [r.pRevert]))])].sort((a, b) => a - b);
  const cloudRecall = comparePolicies(devEdits, local, cloud, { lo: 0, hi: 0 }, prices)[2]!.recall.rate;
  let best: { band: Band; result: PolicyResult } | undefined;
  for (const lo of edges) {
    for (const hi of edges) {
      if (hi <= lo) continue;
      const result = comparePolicies(devEdits, local, cloud, { lo, hi }, prices)[3]!;
      if (result.recall.rate < target * cloudRecall) continue;
      if (!best || result.escalated < best.result.escalated || (result.escalated === best.result.escalated && result.f1 > best.result.f1)) {
        best = { band: { lo, hi }, result };
      }
    }
  }
  if (!best) throw new Error('no band reaches the recall target on dev');
  return { ...best, cloudRecall };
}

/** USD per 1,000 classifiable edits: the filter removes `1 - survivorShare` of them for free. */
export const perThousandEdits = (costPerModelEdit: number, survivorShare: number) => 1000 * survivorShare * costPerModelEdit;
