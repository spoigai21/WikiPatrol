// Synthetic EventStreams events for labeller tests.

let n = 0;
const iso = (ms: number) => new Date(ms).toISOString();
export const edit = (revId: number, atMs: number, extra: Record<string, unknown> = {}) =>
  JSON.stringify({
    meta: { id: `rc-${n++}`, dt: iso(atMs), stream: 'mediawiki.recentchange' },
    type: 'edit', wiki: 'enwiki', namespace: 0, title: `Page ${revId}`, user: 'Someone', bot: false,
    timestamp: Math.floor(atMs / 1000), revision: { old: revId - 1, new: revId }, ...extra,
  });
export const tagged = (revId: number, atMs: number, tags: string[], prior: string[] = []) =>
  JSON.stringify({
    meta: { id: `tc-${n++}`, dt: iso(atMs), stream: 'mediawiki.revision-tags-change' },
    database: 'enwiki', rev_id: revId, rev_timestamp: iso(atMs), page_namespace: 0, tags, prior_state: { tags: prior },
  });
/** Unrelated traffic that only moves event time forward, one event a minute. */
export const tick = (atMs: number) =>
  JSON.stringify({ meta: { id: `tick-${n++}`, dt: iso(atMs), stream: 'mediawiki.recentchange' }, type: 'edit', wiki: 'dewiki', namespace: 0 });

export const ticks = (fromMs: number, toMs: number) => {
  const out: string[] = [];
  for (let t = fromMs; t <= toMs; t += 60_000) out.push(tick(t));
  return out;
};
