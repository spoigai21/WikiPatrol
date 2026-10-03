import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fetchDiff, retryPolicy } from '../src/labels/action-api.ts';

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });

describe('Action API diffs', () => {
  beforeEach(() => { retryPolicy.baseMs = 1; });
  afterEach(() => { vi.unstubAllGlobals(); retryPolicy.baseMs = 2000; });

  it('retries rate limits, server errors and maxlag, then returns the diff', async () => {
    const responses = [
      new Response('slow down', { status: 429 }),
      new Response('oops', { status: 503 }),
      json({ error: { code: 'maxlag', info: 'lagged' } }),
      json({ compare: { body: '<td class="diff-addedline">x</td>' } }),
    ];
    const fetchMock = vi.fn(async () => responses.shift()!);
    vi.stubGlobal('fetch', fetchMock);
    expect(await fetchDiff(1, false)).toEqual({ kind: 'diff', html: '<td class="diff-addedline">x</td>' });
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it('reports content the API says is gone as unavailable', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => json({ error: { code: 'missingcontent', info: 'Missing content' } })));
    expect(await fetchDiff(1, false)).toEqual({ kind: 'unavailable', reason: 'Action API missingcontent: Missing content' });
  });

  it('throws on a failure that does not clear, rather than freezing it as unavailable', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('down', { status: 502 })));
    await expect(fetchDiff(1, false)).rejects.toThrow(/still failing/);
  });
});
