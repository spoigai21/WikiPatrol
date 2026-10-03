// Batched, polite reads from the MediaWiki Action API: current revision tags (the revert label)
// and account facts (the filter's user snapshot). Free and unauthenticated.

import { USER_AGENT } from '../stream/sse.ts';
import type { UserSnapshot } from '../filter/rules.ts';

export const ENWIKI_API = 'https://en.wikipedia.org/w/api.php';
const BATCH = 50;
const PAUSE_MS = 200;

/** An answer from the API about the content itself (deleted, hidden) — not a transport failure. */
export class ApiError extends Error {
  constructor(readonly code: string, info: string) {
    super(`Action API ${code}: ${info}`);
  }
}

/** Exported so tests can shorten the waits. */
export const retryPolicy = { retries: 6, baseMs: 2000 };

/**
 * GET with retries: rate limiting (429), server errors and `maxlag` are temporary and retried with
 * backoff; anything still failing after that throws, so a caller never mistakes it for an answer.
 */
async function get<T>(api: string, params: Record<string, string>): Promise<T> {
  const url = new URL(api);
  url.search = new URLSearchParams({ format: 'json', formatversion: '2', maxlag: '5', ...params }).toString();
  for (let attempt = 0; ; attempt++) {
    let transient: string;
    try {
      const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT } });
      if (res.ok) {
        const body = (await res.json()) as T & { error?: { code: string; info: string } };
        if (!body.error) return body;
        if (body.error.code !== 'maxlag') throw new ApiError(body.error.code, body.error.info);
        transient = 'maxlag';
      } else if (res.status === 429 || res.status >= 500) transient = `HTTP ${res.status}`;
      else throw new Error(`Action API HTTP ${res.status}`);
    } catch (err) {
      if (err instanceof ApiError || (err instanceof Error && err.message.startsWith('Action API HTTP'))) throw err;
      transient = err instanceof Error ? err.message : String(err);
    }
    if (attempt >= retryPolicy.retries) throw new Error(`Action API still failing after ${retryPolicy.retries} retries: ${transient}`);
    await new Promise((r) => setTimeout(r, retryPolicy.baseMs * 2 ** attempt));
  }
}

async function inBatches<T>(items: readonly T[], fn: (batch: T[]) => Promise<void>): Promise<void> {
  for (let i = 0; i < items.length; i += BATCH) {
    await fn(items.slice(i, i + BATCH));
    if (i + BATCH < items.length) await new Promise((r) => setTimeout(r, PAUSE_MS));
  }
}

/**
 * Current tags for each revision. A revision missing from the result (deleted page, suppressed
 * revision) is absent from the map: it has no revert tag, but it was not "kept" either.
 */
export async function fetchRevisionTags(revIds: readonly number[], api = ENWIKI_API): Promise<Map<number, string[]>> {
  const out = new Map<number, string[]>();
  await inBatches(revIds, async (batch) => {
    const body = await get<{ query?: { pages?: { revisions?: { revid: number; tags?: string[] }[] }[] } }>(api, {
      action: 'query',
      prop: 'revisions',
      revids: batch.join('|'),
      rvprop: 'ids|tags',
    });
    for (const page of body.query?.pages ?? []) for (const r of page.revisions ?? []) out.set(r.revid, r.tags ?? []);
  });
  return out;
}

/** Registration date and edit count per account, as of now. Unknown accounts are absent. */
export async function fetchUsers(names: readonly string[], api = ENWIKI_API): Promise<Map<string, UserSnapshot>> {
  const out = new Map<string, UserSnapshot>();
  await inBatches(names, async (batch) => {
    const body = await get<{
      query?: { users?: { name: string; missing?: boolean; invalid?: boolean; registration?: string | null; editcount?: number }[] };
    }>(api, { action: 'query', list: 'users', ususers: batch.join('|'), usprop: 'registration|editcount' });
    for (const u of body.query?.users ?? []) {
      if (u.missing || u.invalid) continue;
      out.set(u.name, { registration: u.registration ?? null, editcount: u.editcount ?? 0 });
    }
  });
  return out;
}

export type DiffResult =
  | { kind: 'diff'; html: string }
  | { kind: 'created'; text: string }
  | { kind: 'unavailable'; reason: string };

/**
 * The change a revision made, as the compare API's table diff. A page creation has no parent to
 * compare with, so its full text is returned instead. Revision-deleted or suppressed content is
 * `unavailable` — a model gets told so rather than an empty diff. Network failures throw.
 */
export async function fetchDiff(revId: number, isNewPage: boolean, api = ENWIKI_API): Promise<DiffResult> {
  try {
    if (isNewPage) {
      const body = await get<{ query?: { pages?: { revisions?: { slots?: { main?: { content?: string; texthidden?: boolean } } }[] }[] } }>(api, {
        action: 'query',
        prop: 'revisions',
        revids: String(revId),
        rvprop: 'content',
        rvslots: 'main',
      });
      const main = body.query?.pages?.[0]?.revisions?.[0]?.slots?.main;
      if (!main || main.texthidden || main.content === undefined) return { kind: 'unavailable', reason: 'content hidden or missing' };
      return { kind: 'created', text: main.content };
    }
    const body = await get<{ compare?: { body?: string } }>(api, { action: 'compare', fromrev: String(revId), torelative: 'prev', prop: 'diff' });
    return { kind: 'diff', html: body.compare?.body ?? '' };
  } catch (err) {
    // Only the API's own word that the content is gone counts; transport failures propagate.
    if (err instanceof ApiError) return { kind: 'unavailable', reason: err.message };
    throw err;
  }
}
