import { describe, expect, it } from 'vitest';
import { dlqStage, enrichStage, filterStage, parseStage, type EditRecord } from '../src/pipeline/stages.ts';
import { edit, tagged } from './events-fixture.ts';

const T0 = Date.parse('2026-09-30T12:00:00Z');
const msgs = (values: string[]) => values.map((value, i) => ({ value, offset: String(i) }));
const outs = async (r: Promise<{ offset: string; out: { key: string; value: string }[] }[]> | { offset: string; out: { key: string; value: string }[] }[]) =>
  (await r).flatMap((x) => x.out.map((o) => ({ offset: x.offset, ...o })));

describe('pipeline stages', () => {
  it('parse keeps classifiable enwiki edits only, as identical bytes every time', async () => {
    const input = msgs([edit(1, T0), edit(2, T0, { wiki: 'dewiki' }), edit(3, T0, { namespace: 1 }), tagged(1, T0, ['mw-reverted']), 'not json']);
    const a = await outs(parseStage().process(input));
    const b = await outs(parseStage().process(input));
    expect(a.map((o) => o.offset)).toEqual(['0']);
    expect(JSON.parse(a[0]!.value)).toMatchObject({ revId: 1, wiki: 'enwiki', userClass: 'registered', isNew: false });
    expect(b).toEqual(a);
  });

  it('dlq keeps what nothing can read, with the reason', async () => {
    const input = msgs([edit(1, T0), tagged(1, T0, []), 'not json', JSON.stringify({ meta: { stream: 'mediawiki.recentchange', id: 'x' } }), JSON.stringify({ meta: { stream: 'other', id: 'y' } })]);
    const out = await outs(dlqStage().process(input));
    expect(out.map((o) => [o.offset, JSON.parse(o.value).reason.split(':')[0]])).toEqual([
      ['2', 'not JSON'],
      ['3', 'recentchange schema'],
      ['4', 'unknown stream'],
    ]);
    expect(JSON.parse(out[0]!.value).raw).toBe('not json');
  });

  it('enrich records the account it saw, and caches lookups', async () => {
    const rec = (revId: number, user: string, userClass: EditRecord['userClass'] = 'registered') =>
      JSON.stringify({ revId, wiki: 'enwiki', title: 't', timestamp: T0 / 1000, userClass, user, minor: false, isNew: false, lengthOld: 1, lengthNew: 2, comment: '', metaId: 'm' } satisfies EditRecord);
    let calls = 0;
    const stage = enrichStage({
      now: () => T0,
      lookup: async (names) => { calls++; return new Map(names.filter((n) => n !== 'Gone').map((n) => [n, { registration: '2020-01-01T00:00:00Z', editcount: 900 }])); },
    });
    const out = await outs(stage.process(msgs([rec(1, 'Alice'), rec(2, 'Alice'), rec(3, 'Gone'), rec(4, '~2026-1', 'temporary')])));
    expect(out.map((o) => JSON.parse(o.value).account)).toEqual([{ registration: '2020-01-01T00:00:00Z', editcount: 900 }, { registration: '2020-01-01T00:00:00Z', editcount: 900 }, null, null]);
    await stage.process(msgs([rec(5, 'Alice')]));
    expect(calls).toBe(1);
  });

  it('filter decides from the recorded account, deterministically', async () => {
    const enriched = (account: unknown, userClass = 'registered') =>
      JSON.stringify({ revId: 1, wiki: 'enwiki', title: 't', timestamp: T0 / 1000, userClass, user: 'u', minor: false, isNew: false, lengthOld: 1, lengthNew: 2, comment: '', metaId: 'm', account, accountAsOf: null });
    const out = await outs(filterStage().process(msgs([enriched({ registration: '2020-01-01T00:00:00Z', editcount: 900 }), enriched(null, 'temporary'), enriched(null, 'bot')])));
    expect(out.map((o) => JSON.parse(o.value).filter)).toEqual([
      { policy: 'extendedconfirmed', keep: false, rule: 'trusted-account' },
      { policy: 'extendedconfirmed', keep: true, rule: 'kept' },
      { policy: 'extendedconfirmed', keep: false, rule: 'bot' },
    ]);
  });
});

import { chunkBySize } from '../src/kafka/topics.ts';

describe('chunkBySize', () => {
  it('keeps order and keeps every chunk under the limit', () => {
    const msgs = Array.from({ length: 50 }, (_, i) => ({ key: String(i), value: 'x'.repeat(1000) }));
    const chunks = chunkBySize(msgs, 10_000);
    expect(chunks.flat()).toEqual(msgs);
    for (const c of chunks) expect(c.reduce((a, m) => a + m.value.length + 64 + m.key.length, 0)).toBeLessThanOrEqual(10_000);
    expect(chunks.length).toBeGreaterThan(1);
  });
  it('gives an oversized message a chunk of its own', () => {
    expect(chunkBySize([{ key: 'a', value: 'x'.repeat(50) }, { key: 'b', value: 'y' }], 20).map((c) => c.length)).toEqual([1, 1]);
  });
});
