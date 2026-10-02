// Phase 0, step 3a: draw the stratified label-noise sample from captured edits.
//
// Takes main-namespace, non-bot enwiki edits that are at least --min-age-hours old
// (so reverts have had time to happen), asks the Action API for each revision's
// current tags, and writes:
//   labels/sample.csv                 the sheet to hand-label (no revert column)
//   results/phase0/sample-key.json    which rows were reverted, plus population stats
//
//   npm run phase0:sample -- --min-age-hours 24

import { readdirSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { USER_AGENT } from '../stream/sse.ts';
import { classifyUser, isClassifiable, RecentChange, REVERT_TAGS } from './events.ts';
import { args, log, writeJson } from './cli.ts';
import { toCsv } from './csv.ts';
import { isReverted, stratify, type Candidate } from './sample.ts';
import { mulberry32, round, shuffled, wilson } from './stats.ts';

const opts = args({
  'min-age-hours': { type: 'string', default: '24' },
  'per-stratum': { type: 'string', default: '50' },
  'max-query': { type: 'string', default: '3000' },
  seed: { type: 'string', default: '20261002' },
  wiki: { type: 'string', default: 'enwiki' },
  api: { type: 'string', default: 'https://en.wikipedia.org/w/api.php' },
});
const minAgeHours = Number(opts['min-age-hours']);
const perStratum = Number(opts['per-stratum']);
const maxQuery = Number(opts['max-query']);
const seed = Number(opts.seed);
const now = Date.now();

// 1. Eligible edits from every capture file.
const captures = readdirSync('data/phase0').filter((f) => f.startsWith('capture-') && f.endsWith('.jsonl'));
const eligible = new Map<number, Omit<Candidate, 'revertTags'>>();
let tooYoung = 0;
for (const file of captures) {
  for (const line of readFileSync(`data/phase0/${file}`, 'utf8').split('\n')) {
    if (!line) continue;
    const p = RecentChange.safeParse(JSON.parse(line));
    if (!p.success) continue;
    const rc = p.data;
    if (rc.wiki !== opts.wiki || !isClassifiable(rc) || rc.bot || !rc.revision) continue;
    const ts = Date.parse(rc.meta.dt);
    if (now - ts < minAgeHours * 3_600_000) {
      tooYoung++;
      continue;
    }
    eligible.set(rc.revision.new, {
      revId: rc.revision.new,
      title: rc.title ?? '',
      userClass: classifyUser(rc.user, rc.bot),
      timestamp: rc.meta.dt,
    });
  }
}
log(`${captures.length} capture files, ${eligible.size} eligible edits, ${tooYoung} younger than ${minAgeHours}h`);
if (eligible.size === 0) {
  log('nothing eligible yet; wait until captures are old enough');
  process.exit(1);
}

// 2. Current tags for a random subset (the API is free, but be polite).
const toQuery = shuffled([...eligible.values()], mulberry32(seed)).slice(0, maxQuery);
const candidates: Candidate[] = [];
let gone = 0;
for (let i = 0; i < toQuery.length; i += 50) {
  const batch = toQuery.slice(i, i + 50);
  const url = new URL(String(opts.api));
  url.search = new URLSearchParams({
    action: 'query',
    prop: 'revisions',
    revids: batch.map((c) => c.revId).join('|'),
    rvprop: 'ids|tags',
    format: 'json',
    formatversion: '2',
    maxlag: '5',
  }).toString();
  const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT } });
  if (!res.ok) throw new Error(`Action API HTTP ${res.status}`);
  const body = (await res.json()) as {
    query?: { pages?: { revisions?: { revid: number; tags?: string[] }[] }[]; badrevids?: Record<string, unknown> };
  };
  const tags = new Map<number, string[]>();
  for (const page of body.query?.pages ?? []) for (const r of page.revisions ?? []) tags.set(r.revid, r.tags ?? []);
  for (const c of batch) {
    const t = tags.get(c.revId);
    // Missing = deleted page or suppressed revision. Often vandalism, but it has no
    // revert tag, so it belongs in neither stratum; it is counted instead.
    if (t === undefined) {
      gone++;
      continue;
    }
    candidates.push({ ...c, revertTags: t.filter((x) => (REVERT_TAGS as readonly string[]).includes(x)) });
  }
  await new Promise((r) => setTimeout(r, 200));
}

const nReverted = candidates.filter(isReverted).length;
log(`${candidates.length} with tags, ${nReverted} reverted, ${gone} deleted/suppressed`);

// 3. Stratify and write.
const { ordered } = stratify(candidates, perStratum, seed);
const reverted = new Set(candidates.filter(isReverted).map((c) => c.revId));

mkdirSync('labels', { recursive: true });
writeFileSync(
  'labels/sample.csv',
  toCsv(
    ['row', 'rev_id', 'title', 'user_class', 'diff_url', 'vandalism', 'notes'],
    ordered.map((c, i) => [i + 1, c.revId, c.title, c.userClass, `https://en.wikipedia.org/w/index.php?diff=${c.revId}`, '', '']),
  ),
);

writeJson('results/phase0/sample-key.json', {
  builtAt: new Date(now).toISOString(),
  seed,
  revertWindowHours: `at least ${minAgeHours} (each edit's age at query time varies; see timestamps)`,
  population: {
    eligibleEdits: eligible.size,
    queried: toQuery.length,
    deletedOrSuppressed: gone,
    withTags: candidates.length,
    reverted: nReverted,
    // Main namespace, non-bot, enwiki. Deleted/suppressed edits excluded from the denominator.
    revertRate: wilson(nReverted, candidates.length),
    deletedOrSuppressedRate: round(gone / toQuery.length),
  },
  blindingNote:
    'labels/sample.csv omits the revert column, but Wikipedia diff pages display a "Reverted" tag. Judge the content, not the tag.',
  rows: ordered.map((c, i) => ({ row: i + 1, revId: c.revId, reverted: reverted.has(c.revId), revertTags: c.revertTags, timestamp: c.timestamp })),
});

log(`wrote labels/sample.csv (${ordered.length} rows) and results/phase0/sample-key.json`);
log('label each row in the vandalism column: y = vandalism, n = not vandalism, u = unsure');
