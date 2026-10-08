import { describe, expect, it } from 'vitest';
import { countKept } from '../src/phase8/kept.ts';
import { attachKept, correlation, toMinutes, windows, type Sample } from '../src/phase8/report.ts';

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

  it('counts kept edits per minute from broker timestamps, skipping bad lines', () => {
    const at = (min: number, sec: number) => Date.parse('2026-10-05T00:00:00Z') + min * 60_000 + sec * 1000;
    const rec = (keep: boolean) => JSON.stringify({ revId: 1, filter: { policy: 'p', keep, rule: 'r' } });
    const out = countKept([`${at(0, 5)} ${rec(true)}`, `${at(0, 50)} ${rec(false)}`, `${at(1, 1)} ${rec(true)}`, 'garbage', `${at(1, 2)} {not json`]);
    expect(out).toEqual([[at(0, 0), 2, 1], [at(1, 0), 1, 1]]);
  });

  it('gives each minute the kept rate of the minutes just before it, and windows average it', () => {
    const base = Date.parse('2026-10-05T00:00:00Z');
    const perMinute = Array.from({ length: 70 }, (_, i) => [base + i * 60_000, 50, i < 35 ? 10 : 20] as [number, number, number]);
    const ms = attachKept(toMinutes(Array.from({ length: 70 }, (_, i) => s(i, i * 50))), perMinute);
    expect(ms[10]!.kept).toBe(10);
    expect(ms[40]!.kept).toBe(20);
    // ms[i] is sample i + 1: ms[36] sits at minute 37 and averages minutes 32-36.
    expect(ms[36]!.kept).toBe((3 * 10 + 2 * 20) / 5);
    expect(windows(ms, 30)[0]!.kept).toBeGreaterThan(9);
  });

  it('correlation', () => {
    expect(correlation([1, 2, 3, 4], [2, 4, 6, 8])).toBe(1);
    expect(correlation([1, 2, 3, 4], [8, 6, 4, 2])).toBe(-1);
    expect(Number.isNaN(correlation([1, 2], [1, 2]))).toBe(true);
    expect(Number.isNaN(correlation([1, 2, 3], [5, 5, 5]))).toBe(true);
  });
});
