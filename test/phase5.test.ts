import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { QuotaExhausted, RetryLater } from '../src/models/clients.ts';
import type { SetEdit } from '../src/phase5/build-set.ts';
import { renderDiff } from '../src/phase5/diff.ts';
import type { Prediction, Predictor } from '../src/phase5/predictors.ts';
import { buildMessages, parseAnswer, renderEdit } from '../src/phase5/prompts.ts';
import { readRun, runPredictor } from '../src/phase5/runner.ts';
import { scoreRun } from '../src/phase5/score.ts';

const edit = (revId: number, label: SetEdit['label'] = 'not-reverted', extra: Partial<SetEdit> = {}): SetEdit => ({
  revId, title: `Page ${revId}`, editTime: '2026-09-30T10:00:00.000Z', isNew: false, minor: false, userClass: 'temporary',
  accountAgeDays: null, editcount: null, sizeDelta: 12, comment: 'fix', diff: '- a\n+ b', diffTruncated: false, label, ...extra,
});

describe('diff rendering', () => {
  it('marks only the changed words, with context', () => {
    const html =
      '<tr><td class="diff-deletedline diff-side-deleted"><div>The cat sat on <del class="diffchange diffchange-inline">the</del> mat.</div></td>' +
      '<td class="diff-addedline diff-side-added"><div>The cat sat on <ins class="diffchange diffchange-inline">a red</ins> mat.</div></td></tr>' +
      '<tr><td class="diff-addedline"><div>A whole new &amp; line</div></td></tr>';
    expect(renderDiff(html).text).toBe('- …The cat sat on [-the-] mat.…\n+ …The cat sat on {+a red+} mat.…\n+ A whole new & line');
  });

  it('caps the length and says so', () => {
    const html = `<td class="diff-addedline"><div>${'x'.repeat(5000)}</div></td>`;
    const r = renderDiff(html, 100);
    expect(r.truncated).toBe(true);
    expect(r.text.endsWith('[diff truncated]')).toBe(true);
  });
});

describe('prompts', () => {
  it('never show the label', () => {
    for (const label of ['reverted', 'not-reverted'] as const) {
      for (const p of ['p1-plain', 'p2-guide', 'p3-reason'] as const) {
        const m = buildMessages(p, edit(1, label));
        expect(m.user).not.toMatch(/not-reverted|"label"/);
        expect(m.user).toContain('Article: Page 1');
      }
    }
  });

  it('describe the editor and the change', () => {
    const e = edit(2, 'not-reverted', { userClass: 'registered', accountAgeDays: 12.5, editcount: 40, sizeDelta: -300, comment: '' });
    const text = renderEdit(e);
    expect(text).toContain('Editor: registered account, 12 days old, 40 edits');
    expect(text).toContain('Size change: -300 bytes');
    expect(text).toContain('Edit summary: (none)');
    expect(renderEdit({ ...e, diffUnavailable: 'hidden', diff: '' })).toContain('[the diff is not available]');
  });

  it('accept only the JSON contract', () => {
    expect(parseAnswer('{"revert": true, "p_revert": 0.8}')).toEqual({ ok: true, answer: { revert: true, p_revert: 0.8 } });
    expect(parseAnswer('```json\n{"reason":"x","revert":false,"p_revert":0.1}\n```').ok).toBe(true);
    expect(parseAnswer('I think yes').ok).toBe(false);
    expect(parseAnswer('{"revert": "yes", "p_revert": 0.8}').ok).toBe(false);
    expect(parseAnswer('{"revert": true, "p_revert": 1.5}').ok).toBe(false);
  });
});

const fake = (behaviour: (e: SetEdit, call: number) => Prediction | Error, usesPrompt = true): Predictor & { calls: number } => {
  const p = {
    config: 'fake:model__p1-plain', minIntervalMs: 0, usesPrompt, calls: 0,
    async predict(e: SetEdit) {
      const r = behaviour(e, p.calls++);
      if (r instanceof Error) throw r;
      return r;
    },
  };
  return p;
};
const answer = (e: SetEdit, revert: boolean | null): Prediction => ({
  revId: e.revId, revert, pRevert: revert === null ? null : revert ? 0.9 : 0.1, ...(revert === null ? { invalid: 'not JSON' } : {}),
  latencyMs: 5, tokensIn: 100, tokensOut: 10, modelVersion: 'fake-1',
});
const tmp = () => join(mkdtempSync(join(tmpdir(), 'wp5-')), 'run.jsonl');
const noSleep = async () => {};

describe('runner', () => {
  const edits = [1, 2, 3, 4, 5].map((i) => edit(i, i <= 2 ? 'reverted' : 'not-reverted'));

  it('appends one row per edit, including invalid answers, and resumes where it stopped', async () => {
    const out = tmp();
    const ctl = new AbortController();
    const p = fake((e, call) => {
      if (call === 2) ctl.abort();
      return answer(e, e.revId === 3 ? null : e.revId <= 2);
    });
    const first = await runPredictor({ set: 'dev', edits, predictor: p, out, signal: ctl.signal, sleep: noSleep });
    expect(first.stopped).toBe('aborted');
    expect(readRun(out).map((r) => r.revId)).toEqual([1, 2, 3]);

    const second = await runPredictor({ set: 'dev', edits, predictor: p, out, sleep: noSleep });
    expect(second).toEqual({ done: 5, total: 5, written: 2 });
    const rows = readRun(out);
    expect(rows.map((r) => r.revId)).toEqual([1, 2, 3, 4, 5]);
    expect(rows[2]).toMatchObject({ revert: null, invalid: 'not JSON' });
  });

  it('retries a temporary failure, stops cleanly on a spent quota, and writes nothing for a hard error', async () => {
    const out = tmp();
    let fails = 1;
    const p = fake((e) => {
      if (e.revId === 2 && fails-- > 0) return new RetryLater('429', 1);
      if (e.revId === 4) return new QuotaExhausted('daily');
      return answer(e, false);
    });
    const r = await runPredictor({ set: 'dev', edits, predictor: p, out, sleep: noSleep });
    expect(r.stopped).toBe('quota');
    expect(readRun(out).map((x) => x.revId)).toEqual([1, 2, 3]);

    const out2 = tmp();
    const broken = fake((e) => (e.revId === 2 ? new Error('400 bad request') : answer(e, false)));
    expect((await runPredictor({ set: 'dev', edits, predictor: broken, out: out2, sleep: noSleep })).stopped).toBe('error');
    expect(readRun(out2).map((x) => x.revId)).toEqual([1]);
  });

  it('on the sealed set: refuses changed prompts, and refuses to score a finished configuration twice', async () => {
    const out = tmp();
    const p = fake((e) => answer(e, false));
    await expect(runPredictor({ set: 'sealed', edits, predictor: p, out, sleep: noSleep, checkSealed: () => ['prompts/p1-plain.txt'] })).rejects.toThrow(/differ from their tag/);
    expect(readRun(out)).toEqual([]);

    await runPredictor({ set: 'sealed', edits, predictor: p, out, sleep: noSleep, checkSealed: () => [] });
    await expect(runPredictor({ set: 'sealed', edits, predictor: p, out, sleep: noSleep, checkSealed: () => [] })).rejects.toThrow(/scored once/);
    expect(readFileSync(out, 'utf8').trim().split('\n')).toHaveLength(5);
  });
});

describe('scoring', () => {
  it('computes revert precision and recall, counting invalid answers as not flagged', () => {
    const edits = [edit(1, 'reverted'), edit(2, 'reverted'), edit(3, 'reverted'), edit(4), edit(5), edit(6)];
    const rows = [
      answer(edits[0]!, true), answer(edits[1]!, null), answer(edits[2]!, false),
      answer(edits[3]!, true), answer(edits[4]!, false), answer(edits[5]!, false),
    ].map((p) => ({ ...p, config: 'c', set: 'dev', at: '' }));
    const s = scoreRun(edits, rows, 'c');
    expect(s.precision.rate).toBe(0.5); // 1 of 2 flagged was reverted
    expect(s.recall.rate).toBeCloseTo(1 / 3, 4); // 1 of 3 reverted was flagged; the invalid one counts as missed
    expect(s.invalid).toBeCloseTo(1 / 6, 4);
    expect(s.complete).toBe(true);
    expect(s.tokensPerEdit).toEqual({ in: 100, out: 10 });
  });
});
