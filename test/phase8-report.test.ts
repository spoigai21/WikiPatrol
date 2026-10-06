import { describe, expect, it } from 'vitest';
import { correlation, toMinutes, windows, type Sample } from '../src/phase8/report.ts';

const s = (min: number, scored: number, replicas = 3, lag = 10): Sample => ({
  t: new Date(Date.parse('2026-10-05T00:00:00Z') + min * 60_000).toISOString(), replicas, desired: replicas, metric: 0, classifierLag: lag, raw: 0, scored, predictions: 0,
});

describe('phase 8 report', () => {
  it('turns cumulative topic sizes into a per-minute feed, never across a gap', () => {
    const ms = toMinutes([s(0, 100), s(1, 160), s(2, 230), s(30, 2000), s(31, 2050)]);
    expect(ms.map((m) => Math.round(m.feed))).toEqual([60, 70, 50]);
  });

  it('averages fixed windows and drops thinly covered ones', () => {
    const samples = Array.from({ length: 61 }, (_, i) => s(i, i * 50, i < 30 ? 2 : 5));
    const w = windows(toMinutes(samples), 30);
    expect(w).toHaveLength(2);
    expect(w[0]!.feed).toBe(50);
    expect(w[1]!.replicas).toBe(5);
    expect(windows(toMinutes(samples.slice(0, 10)), 30)).toEqual([]);
  });

  it('correlation', () => {
    expect(correlation([1, 2, 3, 4], [2, 4, 6, 8])).toBe(1);
    expect(correlation([1, 2, 3, 4], [8, 6, 4, 2])).toBe(-1);
    expect(Number.isNaN(correlation([1, 2], [1, 2]))).toBe(true);
    expect(Number.isNaN(correlation([1, 2, 3], [5, 5, 5]))).toBe(true);
  });
});
