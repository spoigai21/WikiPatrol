import { describe, expect, it } from 'vitest';
import type { SetEdit } from '../src/phase5/build-set.ts';
import type { Prediction, Predictor } from '../src/phase5/predictors.ts';
import type { RunRow } from '../src/phase5/runner.ts';
import { chooseSubset, compare, degrade } from '../src/phase9/drift.ts';

const edit = (revId: number) => ({ revId }) as SetEdit;
const pred = (revId: number, revert: boolean | null, version = 'm@1'): Prediction => ({ revId, revert, pRevert: revert === null ? null : revert ? 0.8 : 0.2, latencyMs: 1, tokensIn: 1, tokensOut: 1, modelVersion: version });
const row = (p: Prediction): RunRow => ({ ...p, config: 'c', set: 'sealed', at: '' });

describe('drift', () => {
  it('chooses the same subset every time', () => {
    const edits = Array.from({ length: 500 }, (_, i) => edit(1000 + i));
    expect(chooseSubset(edits)).toEqual(chooseSubset([...edits].reverse()));
    expect(new Set(chooseSubset(edits)).size).toBe(50);
  });

  it('is quiet when nothing changed', () => {
    const before = [1, 2, 3, 4].map((i) => row(pred(i, i % 2 === 0)));
    const c = compare('c', before, before);
    expect(c.agreement).toBe(1);
    expect(c.alerts).toEqual([]);
  });

  it('alerts on flipped decisions, a new model version, and more invalid answers', () => {
    const before = Array.from({ length: 10 }, (_, i) => row(pred(i, i < 5)));
    const flipped = before.map((r, i) => pred(r.revId, i < 2 ? !r.revert : r.revert));
    expect(compare('c', before, flipped).alerts[0]).toMatch(/agreement 80.0% is below 90%/);
    expect(compare('c', before, before.map((r) => pred(r.revId, r.revert, 'm@2'))).alerts[0]).toMatch(/model version changed: m@1 -> m@2/);
    expect(compare('c', before, before.map((r, i) => pred(r.revId, i === 0 ? null : r.revert))).alerts.join()).toMatch(/invalid answers rose/);
  });

  it('a degraded configuration trips the alert, deterministically', async () => {
    const inner: Predictor = { config: 'c', minIntervalMs: 0, usesPrompt: true, predict: async (e) => pred(e.revId, e.revId % 2 === 0) };
    const edits = Array.from({ length: 50 }, (_, i) => edit(i));
    const before = await Promise.all(edits.map((e) => inner.predict(e))).then((ps) => ps.map(row));
    const bad = degrade(inner, 30);
    const run1 = await Promise.all(edits.map((e) => bad.predict(e)));
    const run2 = await Promise.all(edits.map((e) => bad.predict(e)));
    expect(run2).toEqual(run1);
    const c = compare('c', before, run1);
    expect(c.flips.length).toBeGreaterThan(5);
    expect(c.alerts[0]).toMatch(/below 90%/);
  });
});
