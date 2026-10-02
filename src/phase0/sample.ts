// Pure logic for the label-noise sample: stratification and scoring.
// Network and file I/O live in build-sample.ts / score-sample.ts.

import { mulberry32, round, shuffled, wilson } from './stats.ts';

export interface Candidate {
  revId: number;
  title: string;
  userClass: string;
  timestamp: string;
  /** Current revert tags on the revision, from the Action API. */
  revertTags: string[];
}

export const isReverted = (c: Candidate) => c.revertTags.includes('mw-reverted');

/** perStratum reverted + perStratum not-reverted, presented in a seeded random order. */
export function stratify(candidates: readonly Candidate[], perStratum: number, seed: number) {
  const rand = mulberry32(seed);
  const reverted = shuffled(candidates.filter(isReverted), rand).slice(0, perStratum);
  const kept = shuffled(candidates.filter((c) => !isReverted(c)), rand).slice(0, perStratum);
  return { reverted, kept, ordered: shuffled([...reverted, ...kept], rand) };
}

export type Judgement = 'y' | 'n' | 'u';

export interface LabelledRow {
  reverted: boolean;
  vandalism: Judgement;
}

/**
 * Both directions of label noise, as the spec requires:
 *  - revertedNotVandalism: P(not vandalism | reverted)   -> the label's false positives
 *  - keptButVandalism:     P(vandalism | not reverted)    -> vandalism the label misses
 * "Unsure" rows are excluded and counted. Overall agreement is reweighted to the
 * population revert rate, because the sample is 50/50 and the population is not.
 */
export function scoreNoise(rows: readonly LabelledRow[], populationRevertRate: number) {
  const sure = rows.filter((r) => r.vandalism !== 'u');
  const rev = sure.filter((r) => r.reverted);
  const kept = sure.filter((r) => !r.reverted);
  const revNotVandal = rev.filter((r) => r.vandalism === 'n').length;
  const keptVandal = kept.filter((r) => r.vandalism === 'y').length;

  const pRevNotVandal = rev.length ? revNotVandal / rev.length : NaN;
  const pKeptVandal = kept.length ? keptVandal / kept.length : NaN;
  const r = populationRevertRate;
  const agreement = r * (1 - pRevNotVandal) + (1 - r) * (1 - pKeptVandal);

  return {
    labelled: rows.length,
    unsure: rows.length - sure.length,
    revertedNotVandalism: wilson(revNotVandal, rev.length),
    keptButVandalism: wilson(keptVandal, kept.length),
    populationRevertRate: round(r),
    populationAgreement: round(agreement),
  };
}
