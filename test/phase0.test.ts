import { describe, expect, it } from 'vitest';
import { addedTags, classifyUser, TagsChange } from '../src/phase0/events.ts';
import { parseCsv, toCsv } from '../src/phase0/csv.ts';
import { scoreNoise, stratify, type Candidate } from '../src/phase0/sample.ts';
import { percentile, summarise, wilson } from '../src/phase0/stats.ts';

describe('classifyUser', () => {
  it.each([
    ['ClueBot NG', true, 'bot'],
    ['192.0.2.1', false, 'ip'],
    ['2001:db8::1', false, 'ip'],
    ['~2025-31415-92', false, 'temporary'],
    ['Jimbo Wales', false, 'registered'],
    [undefined, false, 'unknown'],
  ] as const)('%s (bot=%s) -> %s', (user, bot, want) => {
    expect(classifyUser(user, bot)).toBe(want);
  });
});

describe('addedTags', () => {
  it('returns only tags not present before', () => {
    const tc = TagsChange.parse({
      meta: { id: 'x', dt: '2026-10-02T00:00:00Z', stream: 's' },
      database: 'enwiki',
      rev_id: 1,
      rev_timestamp: '2026-10-02T00:00:00Z',
      page_namespace: 0,
      tags: ['visualeditor', 'mw-reverted'],
      prior_state: { tags: ['visualeditor'] },
    });
    expect(addedTags(tc)).toEqual(['mw-reverted']);
  });
});

describe('stats', () => {
  it('percentile uses nearest rank', () => {
    expect(percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 95)).toBe(10);
    expect(percentile([1, 2, 3, 4], 50)).toBe(2);
  });

  it('summarise handles empty input without throwing', () => {
    expect(summarise([]).n).toBe(0);
  });

  it('wilson matches the textbook value for 5/10', () => {
    const w = wilson(5, 10);
    expect(w.low).toBeCloseTo(0.2366, 3);
    expect(w.high).toBeCloseTo(0.7634, 3);
  });
});

describe('csv', () => {
  it('round-trips commas, quotes and newlines', () => {
    const text = toCsv(['a', 'b'], [['x, "y"', 'line1\nline2']]);
    expect(parseCsv(text)).toEqual([{ a: 'x, "y"', b: 'line1\nline2' }]);
  });
});

const cand = (revId: number, reverted: boolean): Candidate => ({
  revId,
  title: `T${revId}`,
  userClass: 'ip',
  timestamp: '2026-10-02T00:00:00Z',
  revertTags: reverted ? ['mw-reverted'] : [],
});

describe('stratify', () => {
  const pool = Array.from({ length: 400 }, (_, i) => cand(i, i % 10 === 0));

  it('draws equal strata and is reproducible from the seed', () => {
    const a = stratify(pool, 20, 42);
    const b = stratify(pool, 20, 42);
    expect(a.reverted).toHaveLength(20);
    expect(a.kept).toHaveLength(20);
    expect(a.ordered.map((c) => c.revId)).toEqual(b.ordered.map((c) => c.revId));
    expect(new Set(a.ordered.map((c) => c.revId)).size).toBe(40);
  });

  it('takes what exists when a stratum is short', () => {
    expect(stratify(pool, 100, 1).reverted).toHaveLength(40);
  });
});

describe('scoreNoise', () => {
  it('measures both directions and reweights agreement to the population', () => {
    const rows = [
      ...Array.from({ length: 10 }, (_, i) => ({ reverted: true, vandalism: i < 6 ? 'y' : 'n' }) as const),
      ...Array.from({ length: 10 }, (_, i) => ({ reverted: false, vandalism: i < 1 ? 'y' : 'n' }) as const),
      { reverted: false, vandalism: 'u' } as const,
    ];
    const s = scoreNoise(rows, 0.1);
    expect(s.unsure).toBe(1);
    expect(s.revertedNotVandalism.rate).toBe(0.4);
    expect(s.keptButVandalism.rate).toBe(0.1);
    // 0.1 * 0.6 + 0.9 * 0.9
    expect(s.populationAgreement).toBeCloseTo(0.87, 4);
  });
});
