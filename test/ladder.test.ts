import { describe, expect, it } from 'vitest';
import type { SetEdit } from '../src/phase5/build-set.ts';
import type { RunRow } from '../src/phase5/runner.ts';
import { answerCost, chooseBand, comparePolicies, perThousandEdits, type Prices } from '../src/phase7/ladder.ts';

const prices: Prices = { retrievedAt: 'test', models: { 'cloud:m': { input: 1, output: 10 }, 'local:m': { input: 0, output: 0 } } };
const edit = (revId: number, reverted: boolean) => ({ revId, label: reverted ? 'reverted' : 'not-reverted' }) as SetEdit;
const row = (config: string, revId: number, revert: boolean | null, p: number | null, latencyMs = 10): RunRow => ({
  revId, revert, pRevert: p, latencyMs, tokensIn: 1000, tokensOut: 100, modelVersion: 'x', config: `${config}__p`, set: 'dev', at: '',
});

// Four edits; two reverted. The local model is sure and right on 1 and 4, unsure on 2 and 3.
const edits = [edit(1, true), edit(2, true), edit(3, false), edit(4, false)];
const local = [row('local:m', 1, true, 0.95), row('local:m', 2, false, 0.5), row('local:m', 3, true, 0.5), row('local:m', 4, false, 0.05)];
const cloud = [row('cloud:m', 1, true, 0.9, 500), row('cloud:m', 2, true, 0.8, 500), row('cloud:m', 3, false, 0.2, 500), row('cloud:m', 4, false, 0.1, 500)];

describe('the ladder', () => {
  it('prices an answer from its tokens', () => {
    expect(answerCost(cloud[0]!, prices)).toBeCloseTo((1000 * 1 + 100 * 10) / 1e6, 10);
    expect(answerCost(local[0]!, prices)).toBe(0);
  });

  it('escalates only unsure edits, and pays the cloud price only for those', () => {
    const [heur, loc, cl, lad] = comparePolicies(edits, local, cloud, { lo: 0.3, hi: 0.7 }, prices);
    expect(heur!.recall.rate).toBe(1);
    expect(loc!.recall.rate).toBe(0.5);
    expect(cl!.recall.rate).toBe(1);
    expect(lad!.recall.rate).toBe(1);
    expect(lad!.precision.rate).toBe(1);
    expect(lad!.escalated).toBe(0.5);
    expect(lad!.costPerModelEdit).toBeCloseTo(cl!.costPerModelEdit / 2, 10);
    expect(lad!.latencyMs.p95).toBe(510);
  });

  it('escalates invalid local answers', () => {
    const bad = [...local.slice(0, 3), row('local:m', 4, null, null)];
    expect(comparePolicies(edits, bad, cloud, { lo: 0.3, hi: 0.7 }, prices)[3]!.escalated).toBe(0.75);
  });

  it('chooses the cheapest band that keeps the recall target', () => {
    const { band, result } = chooseBand(edits, local, cloud, prices, 1);
    expect(result.recall.rate).toBe(1);
    expect(result.escalated).toBe(0.5);
    expect(band.lo).toBeLessThanOrEqual(0.5);
    expect(band.hi).toBeGreaterThan(0.5);
  });

  it('counts the filter as free', () => {
    expect(perThousandEdits(0.002, 0.33)).toBeCloseTo(0.66, 10);
  });
});
