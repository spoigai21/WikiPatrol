// The pipeline's stages (D13). Each turns one input message into zero or more output records.
//
//   wiki.raw ─┬─ parse  ─> wiki.edits ── enrich ─> wiki.enriched ── filter ─> wiki.scored
//             ├─ dlq    ─> wiki.dlq
//             └─ labeller (src/labels) ─> wiki.labels
//
// parse, dlq and filter are pure: same input bytes, same output bytes. enrich looks accounts up in
// the Action API, which is not repeatable, so it records what it saw (and when) in its output;
// everything downstream reads that record instead of asking again.

import { classifyUser, isClassifiable, RecentChange, TagsChange, type UserClass } from '../phase0/events.ts';
import { decide, DEFAULT_POLICY, type Decision, type Policy, type UserSnapshot } from '../filter/rules.ts';
import { fetchUsers } from '../labels/action-api.ts';
import type { Stage, StageOutput } from '../kafka/stage.ts';

/** wiki.scored is partitioned so the classifier can scale out over it (D14). */
export const SCORED_PARTITIONS = 6;

export const TOPICS = { raw: 'wiki.raw', edits: 'wiki.edits', enriched: 'wiki.enriched', scored: 'wiki.scored', dlq: 'wiki.dlq', labels: 'wiki.labels', predictions: 'wiki.predictions' } as const;

export interface EditRecord {
  revId: number;
  wiki: string;
  title: string;
  /** Unix seconds: the edit's own time. */
  timestamp: number;
  userClass: UserClass;
  user: string;
  minor: boolean;
  isNew: boolean;
  lengthOld: number | null;
  lengthNew: number | null;
  comment: string;
  metaId: string;
}

export interface EnrichedRecord extends EditRecord {
  /** null: not a registered account, or the account was not found. */
  account: UserSnapshot | null;
  /** When the account was looked up. */
  accountAsOf: string | null;
}

export interface ScoredRecord extends EnrichedRecord {
  filter: { policy: Policy } & Decision;
}

type Msg = { value: string; offset: string };
const each = (msgs: readonly Msg[], fn: (m: Msg) => StageOutput[]) => msgs.map((m) => ({ offset: m.offset, out: fn(m) }));

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

/** Fixed key order, so the same edit always serialises to the same bytes. */
export function toEditRecord(rc: RecentChange): EditRecord {
  return {
    revId: rc.revision!.new,
    wiki: rc.wiki,
    title: rc.title ?? '',
    timestamp: rc.timestamp!,
    userClass: classifyUser(rc.user, rc.bot),
    user: rc.user ?? '',
    minor: rc.minor ?? false,
    isNew: rc.type === 'new',
    lengthOld: rc.length?.old ?? null,
    lengthNew: rc.length?.new ?? null,
    comment: rc.comment ?? '',
    metaId: rc.meta.id,
  };
}

export function parseStage(wiki = 'enwiki'): Stage {
  return {
    name: 'parse',
    process: (msgs) =>
      each(msgs, (m) => {
        const json = parseJson(m.value) as { meta?: { stream?: unknown } } | undefined;
        if (json?.meta?.stream !== 'mediawiki.recentchange') return [];
        const p = RecentChange.safeParse(json);
        if (!p.success) return [];
        const rc = p.data;
        if (rc.wiki !== wiki || !isClassifiable(rc) || !rc.revision || rc.timestamp === undefined) return [];
        const rec = toEditRecord(rc);
        return [{ key: String(rec.revId), value: JSON.stringify(rec) }];
      }),
  };
}

/** Raw events no stage can read: kept, with the reason, so nothing disappears silently. */
export function dlqStage(): Stage {
  return {
    name: 'dlq',
    process: (msgs) =>
      each(msgs, (m) => {
        const json = parseJson(m.value) as { meta?: { stream?: unknown; id?: unknown } } | undefined;
        let reason: string | undefined;
        if (json === undefined) reason = 'not JSON';
        else if (json.meta?.stream === 'mediawiki.recentchange') {
          const p = RecentChange.safeParse(json);
          if (!p.success) reason = `recentchange schema: ${p.error.issues[0]?.path.join('.')} ${p.error.issues[0]?.message}`;
        } else if (json.meta?.stream === 'mediawiki.revision-tags-change') {
          const p = TagsChange.safeParse(json);
          if (!p.success) reason = `tags-change schema: ${p.error.issues[0]?.path.join('.')} ${p.error.issues[0]?.message}`;
        } else reason = `unknown stream: ${String(json.meta?.stream)}`;
        if (reason === undefined) return [];
        return [{ key: String(json?.meta?.id ?? `offset-${m.offset}`), value: JSON.stringify({ reason, sourceOffset: m.offset, raw: m.value }) }];
      }),
  };
}

/**
 * Adds an account snapshot to each edit from a registered account. Lookups are batched per input
 * batch and cached for `ttlMs`, so the Action API sees a handful of calls a minute.
 */
export function enrichStage(opts: { ttlMs?: number; lookup?: typeof fetchUsers; now?: () => number } = {}): Stage {
  const ttl = opts.ttlMs ?? 60 * 60_000;
  const lookup = opts.lookup ?? fetchUsers;
  const now = opts.now ?? Date.now;
  const cache = new Map<string, { snap: UserSnapshot | null; at: number }>();
  return {
    name: 'enrich',
    async process(msgs) {
      const recs = msgs.map((m) => ({ offset: m.offset, rec: parseJson(m.value) as EditRecord | undefined }));
      const t = now();
      const want = [...new Set(recs.flatMap(({ rec }) => (rec?.userClass === 'registered' ? [rec.user] : [])))].filter((u) => {
        const c = cache.get(u);
        return !c || t - c.at > ttl;
      });
      if (want.length) {
        const found = await lookup(want);
        for (const u of want) cache.set(u, { snap: found.get(u) ?? null, at: t });
      }
      return recs.map(({ offset, rec }) => {
        if (!rec) return { offset, out: [] };
        const c = rec.userClass === 'registered' ? cache.get(rec.user) : undefined;
        const enriched: EnrichedRecord = { ...rec, account: c?.snap ?? null, accountAsOf: c ? new Date(c.at).toISOString() : null };
        return { offset, out: [{ key: String(rec.revId), value: JSON.stringify(enriched) }] };
      });
    },
  };
}

export function filterStage(policy: Policy = DEFAULT_POLICY): Stage {
  return {
    name: 'filter',
    process: (msgs) =>
      each(msgs, (m) => {
        const rec = parseJson(m.value) as EnrichedRecord | undefined;
        if (!rec) return [];
        const scored: ScoredRecord = { ...rec, filter: { policy, ...decide({ userClass: rec.userClass, timestamp: rec.timestamp }, rec.account ?? undefined, policy) } };
        return [{ key: String(rec.revId), value: JSON.stringify(scored) }];
      }),
  };
}
