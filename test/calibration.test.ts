import { describe, expect, it } from 'vitest';
import { auroc, brier, ece, reliability } from '../src/phase6/calibration.ts';

const items = (pairs: [number, boolean][]) => pairs.map(([p, reverted]) => ({ p, reverted }));

describe('calibration', () => {
  it('a perfectly calibrated model has zero ECE', () => {
    // 0.2 bucket: 1 of 5 reverted; 0.8 bucket: 4 of 5 reverted.
    const xs = items([...Array(5)].map((_, i) => [0.2, i === 0] as [number, boolean]).concat([...Array(5)].map((_, i) => [0.8, i < 4] as [number, boolean])));
    expect(ece(xs)).toBe(0);
    expect(reliability(xs).map((b) => [b.n, b.meanP, b.observed.rate])).toEqual([[5, 0.2, 0.2], [5, 0.8, 0.8]]);
  });

  it('an overconfident model has large ECE', () => {
    const xs = items([[0.9, false], [0.9, false], [0.9, true], [0.9, false]]);
    expect(ece(xs)).toBeCloseTo(0.65, 5);
  });

  it('brier and auroc', () => {
    expect(brier(items([[1, true], [0, false]]))).toBe(0);
    expect(brier(items([[0, true]]))).toBe(1);
    expect(auroc(items([[0.9, true], [0.8, true], [0.2, false], [0.1, false]]))).toBe(1);
    expect(auroc(items([[0.1, true], [0.9, false]]))).toBe(0);
    expect(auroc(items([[0.5, true], [0.5, false]]))).toBe(0.5);
  });

  it('p = 1 falls in the last bucket', () => {
    expect(reliability(items([[1, true]]))[0]).toMatchObject({ from: 0.9, n: 1 });
  });
});
