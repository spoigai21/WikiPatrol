// Phase 9: drift — the system noticing that a model changed behind the same name (D15).
//
// Reruns a fixed subset of the sealed set against each configuration and compares every answer
// with the one the same configuration gave in Phase 5. Inputs are byte-identical, so changed
// answers mean a changed model, provider or plumbing. Monitoring only: nothing here is written to
// the Phase 5 run files, and nothing here is used to tune anything.

import { createHash } from 'node:crypto';
import type { SetEdit } from '../phase5/build-set.ts';
import type { Prediction, Predictor } from '../phase5/predictors.ts';
import type { RunRow } from '../phase5/runner.ts';
import { mulberry32, round, shuffled } from '../phase0/stats.ts';

export const SUBSET_SIZE = 50;
export const SUBSET_SEED = 20261004;

/** The fixed drift subset: a seeded shuffle of the sealed set's rev ids, first SUBSET_SIZE. */
export function chooseSubset(edits: readonly SetEdit[], size = SUBSET_SIZE, seed = SUBSET_SEED): number[] {
  return shuffled(edits.map((e) => e.revId).sort((a, b) => a - b), mulberry32(seed)).slice(0, size);
}

export interface DriftThresholds {
  /** Minimum share of decisions that match the configuration's own Phase 5 answers. */
  minAgreement: number;
  /** Maximum rise in the invalid-answer rate, in absolute terms. */
  maxInvalidRise: number;
}

export const DEFAULT_THRESHOLDS: DriftThresholds = { minAgreement: 0.9, maxInvalidRise: 0.05 };

export interface DriftCheck {
  config: string;
  edits: number;
  agreement: number;
  flips: { revId: number; was: boolean | null; now: boolean | null }[];
  meanAbsDeltaP: number | null;
  invalidBefore: number;
  invalidNow: number;
  versionsBefore: string[];
  versionsNow: string[];
  alerts: string[];
}

/** Compare a rerun with the original answers on the same edits. */
export function compare(config: string, original: readonly RunRow[], rerun: readonly Prediction[], t: DriftThresholds = DEFAULT_THRESHOLDS): DriftCheck {
  const before = new Map(original.map((r) => [r.revId, r]));
  const pairs = rerun.flatMap((n) => {
    const b = before.get(n.revId);
    return b ? [{ b, n }] : [];
  });
  const agree = pairs.filter(({ b, n }) => (b.revert === true) === (n.revert === true)).length;
  const deltas = pairs.flatMap(({ b, n }) => (b.pRevert !== null && n.pRevert !== null ? [Math.abs(b.pRevert - n.pRevert)] : []));
  const invalid = (xs: readonly { revert: boolean | null }[]) => (xs.length ? xs.filter((x) => x.revert === null).length / xs.length : 0);
  const versionsBefore = [...new Set(pairs.map(({ b }) => b.modelVersion))].sort();
  const versionsNow = [...new Set(pairs.map(({ n }) => n.modelVersion))].sort();
  const check: DriftCheck = {
    config,
    edits: pairs.length,
    agreement: round(pairs.length ? agree / pairs.length : NaN),
    flips: pairs.filter(({ b, n }) => (b.revert === true) !== (n.revert === true)).map(({ b, n }) => ({ revId: b.revId, was: b.revert, now: n.revert })),
    meanAbsDeltaP: deltas.length ? round(deltas.reduce((a, d) => a + d, 0) / deltas.length) : null,
    invalidBefore: round(invalid(pairs.map(({ b }) => b))),
    invalidNow: round(invalid(pairs.map(({ n }) => n))),
    versionsBefore,
    versionsNow,
    alerts: [],
  };
  if (pairs.length < rerun.length) check.alerts.push(`${rerun.length - pairs.length} rerun answers have no Phase 5 answer to compare with`);
  if (check.agreement < t.minAgreement) check.alerts.push(`agreement ${(check.agreement * 100).toFixed(1)}% is below ${(t.minAgreement * 100).toFixed(0)}% (${check.flips.length} of ${pairs.length} decisions changed)`);
  if (versionsNow.join() !== versionsBefore.join()) check.alerts.push(`model version changed: ${versionsBefore.join(', ')} -> ${versionsNow.join(', ')}`);
  if (check.invalidNow - check.invalidBefore > t.maxInvalidRise) check.alerts.push(`invalid answers rose from ${(check.invalidBefore * 100).toFixed(1)}% to ${(check.invalidNow * 100).toFixed(1)}%`);
  return check;
}

/**
 * A deliberately degraded configuration, to prove the alert fires: wraps a predictor and flips
 * `percent`% of its decisions, chosen deterministically by rev id. Same config name otherwise, so
 * it is compared with the real configuration's Phase 5 answers.
 */
export function degrade(inner: Predictor, percent: number): Predictor {
  const flip = (revId: number) => createHash('sha256').update(String(revId)).digest()[0]! % 100 < percent;
  return {
    config: inner.config,
    minIntervalMs: inner.minIntervalMs,
    usesPrompt: inner.usesPrompt,
    async predict(e) {
      const p = await inner.predict(e);
      if (!flip(e.revId) || p.revert === null) return p;
      return { ...p, revert: !p.revert, pRevert: p.pRevert === null ? null : round(1 - p.pRevert), modelVersion: p.modelVersion };
    },
  };
}
