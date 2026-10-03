import { describe, expect, it } from 'vitest';
import { Labeller, type Label, type LabellerOptions } from '../src/labels/labeller.ts';
import { edit, tagged, tick, ticks } from './events-fixture.ts';

const H = 3_600_000;
const T0 = Date.parse('2026-09-30T12:00:00Z');
const OPTS: LabellerOptions = { wiki: 'enwiki', windowMs: 72 * H, graceMs: 10 * 60_000, outageGapMs: 2 * 60_000 };
const iso = (ms: number) => new Date(ms).toISOString();

function run(events: string[], opts = OPTS, from = 0) {
  const l = new Labeller(opts);
  const labels: Label[] = [];
  events.forEach((e, i) => { if (i >= from) labels.push(...l.feed(e, String(i))); });
  return { labels, l };
}

describe('revert labeller', () => {
  it('labels a revert inside the window, with its delay', () => {
    const { labels } = run([edit(100, T0), ...ticks(T0, T0 + 5 * H), tagged(100, T0 + 5 * H, ['mw-reverted']), ...ticks(T0 + 5 * H, T0 + 73 * H)]);
    expect(labels).toHaveLength(1);
    expect(labels[0]).toMatchObject({ revId: 100, label: 'reverted', revertDelaySeconds: 5 * 3600, windowHours: 72, sourceOffset: '0' });
  });

  it('a revert after 72h does not count', () => {
    const { labels } = run([edit(100, T0), ...ticks(T0, T0 + 72 * H + 60_000), tagged(100, T0 + 72 * H + 60_000, ['mw-reverted']), ...ticks(T0 + 72 * H + 60_000, T0 + 73 * H)]);
    expect(labels[0]!.label).toBe('not-reverted');
  });

  it('emits nothing until the window plus grace has passed in event time', () => {
    const { labels, l } = run([edit(100, T0), ...ticks(T0, T0 + 72 * H + 9 * 60_000)]);
    expect(labels).toEqual([]);
    expect(l.pending).toBe(1);
    expect(l.feed(tick(T0 + 72 * H + 10 * 60_000), 'x')).toHaveLength(1);
  });

  it('only a newly added mw-reverted counts, not one already present or another tag', () => {
    const { labels } = run([
      edit(100, T0), edit(101, T0),
      ...ticks(T0, T0 + H),
      tagged(100, T0 + H, ['mw-reverted', 'x'], ['mw-reverted']),
      tagged(101, T0 + H, ['mw-undo']),
      ...ticks(T0 + H, T0 + 73 * H),
    ]);
    expect(labels.map((x) => x.label)).toEqual(['not-reverted', 'not-reverted']);
  });

  it('ignores tag changes for edits it never saw, and other wikis', () => {
    const { labels } = run([tagged(999, T0, ['mw-reverted']), edit(100, T0, { wiki: 'dewiki' }), edit(101, T0, { namespace: 1 }), ...ticks(T0, T0 + 73 * H)]);
    expect(labels).toEqual([]);
  });

  it('marks edits on a page deleted within the window', () => {
    const del = JSON.stringify({ meta: { id: 'del', dt: iso(T0 + H), stream: 'mediawiki.recentchange' }, type: 'log', wiki: 'enwiki', namespace: 0, title: 'Page 100', log_type: 'delete', log_action: 'delete' });
    const { labels } = run([edit(100, T0), ...ticks(T0, T0 + H), del, ...ticks(T0 + H, T0 + 73 * H)]);
    expect(labels[0]!.label).toBe('deleted');
  });

  it('a hole in the log inside the window makes the label incomplete, and is reported', () => {
    const { labels, l } = run([edit(100, T0), ...ticks(T0, T0 + H), ...ticks(T0 + 2 * H, T0 + 73 * H)]);
    expect(labels[0]!.label).toBe('incomplete');
    expect(l.stats.outages).toHaveLength(1);
  });

  it('out-of-order events within the grace do not lose a revert', () => {
    const events = [edit(100, T0), ...ticks(T0, T0 + 72 * H + 5 * 60_000), tagged(100, T0 + 72 * H - 60_000, ['mw-reverted']), ...ticks(T0 + 72 * H + 5 * 60_000, T0 + 73 * H)];
    expect(run(events).labels[0]!.label).toBe('reverted');
  });

  it('emits in log order and resuming after the last emitted offset reproduces the rest exactly', () => {
    const events: string[] = [];
    for (let i = 0; i < 50; i++) {
      const at = T0 + i * 30 * 60_000;
      events.push(edit(1000 + i, at), ...ticks(at, at + 29 * 60_000));
      if (i % 3 === 0) events.push(tagged(1000 + i, at + 29 * 60_000, ['mw-reverted']));
    }
    events.push(...ticks(T0 + 25 * H, T0 + 100 * H));
    const whole = run(events).labels;
    expect(whole.map((x) => x.revId)).toEqual(Array.from({ length: 50 }, (_, i) => 1000 + i));
    expect(whole.filter((x) => x.label === 'reverted')).toHaveLength(17);

    // Stop after 20 labels, then restart from the offset after the last one emitted.
    const cut = whole[19]!;
    const resumed = run(events, OPTS, Number(cut.sourceOffset) + 1).labels;
    expect([...whole.slice(0, 20), ...resumed]).toEqual(whole);
  });
});
