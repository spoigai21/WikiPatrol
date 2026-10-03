// Batched, polite reads from the MediaWiki Action API: current revision tags (the revert label)
// and account facts (the filter's user snapshot). Free and unauthenticated.

import { USER_AGENT } from '../stream/sse.ts';
import type { UserSnapshot } from '../filter/rules.ts';

export const ENWIKI_API = 'https://en.wikipedia.org/w/api.php';
const BATCH = 50;
const PAUSE_MS = 200;

async function get<T>(api: string, params: Record<string, string>): Promise<T> {
  const url = new URL(api);
  url.search = new URLSearchParams({ format: 'json', formatversion: '2', maxlag: '5', ...params }).toString();
  const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT } });
  if (!res.ok) throw new Error(`Action API HTTP ${res.status}`);
  const body = (await res.json()) as T & { error?: { code: string; info: string } };
  if (body.error) throw new Error(`Action API ${body.error.code}: ${body.error.info}`);
  return body;
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
