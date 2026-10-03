import { z } from 'zod';

// Only the fields Phase 0 reads. Everything else passes through untouched so
// raw captures stay byte-faithful to the stream.

export const RecentChange = z.looseObject({
  meta: z.looseObject({
    id: z.string(),
    dt: z.string(),
    stream: z.string(),
    topic: z.string().optional(),
    domain: z.string().optional(),
  }),
  type: z.string(),
  wiki: z.string(),
  namespace: z.number().optional(),
  title: z.string().optional(),
  user: z.string().optional(),
  bot: z.boolean().optional(),
  timestamp: z.number().optional(),
  length: z.looseObject({ old: z.number().optional(), new: z.number().optional() }).optional(),
  revision: z.looseObject({ old: z.number().optional(), new: z.number() }).optional(),
  log_type: z.string().nullish(),
  log_action: z.string().nullish(),
});
export type RecentChange = z.infer<typeof RecentChange>;

export const TagsChange = z.looseObject({
  meta: z.looseObject({ id: z.string(), dt: z.string(), stream: z.string() }),
  database: z.string(),
  rev_id: z.number(),
  rev_timestamp: z.string(),
  page_namespace: z.number(),
  tags: z.array(z.string()),
  prior_state: z.looseObject({ tags: z.array(z.string()) }).optional(),
});
export type TagsChange = z.infer<typeof TagsChange>;

export const REVERT_TAGS = ['mw-reverted', 'mw-rollback', 'mw-undo', 'mw-manual-revert'] as const;

export type UserClass = 'bot' | 'ip' | 'temporary' | 'registered' | 'unknown';

const IPV4 = /^(\d{1,3}\.){3}\d{1,3}$/;
const IPV6 = /^[0-9a-f:]+$/i;
// enwiki temporary accounts look like "~2025-31415-92".
const TEMP_ACCOUNT = /^~\d{4}-\d+(-\d+)?$/;

/** Bot first: a bot flag outranks whatever the username looks like. */
export function classifyUser(user: string | undefined, bot: boolean | undefined): UserClass {
  if (bot) return 'bot';
  if (!user) return 'unknown';
  if (TEMP_ACCOUNT.test(user)) return 'temporary';
  if (IPV4.test(user) || (user.includes(':') && IPV6.test(user))) return 'ip';
  return 'registered';
}

/** Edits a classifier would actually see: article-space edits and page creations. */
export function isClassifiable(rc: RecentChange): boolean {
  return (rc.type === 'edit' || rc.type === 'new') && rc.namespace === 0;
}

export function addedTags(tc: TagsChange): string[] {
  const prior = new Set(tc.prior_state?.tags ?? []);
  return tc.tags.filter((t) => !prior.has(t));
}
