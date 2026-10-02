// Phase 0, step 2: where does the revert label actually live?
//
// Listens to recentchange and revision-tags-change on one connection and reports:
//  - whether recentchange events carry a tags field at all
//  - how often each revert tag is *added* to an enwiki revision, and how long after the edit
//  - whether a reverted revision can be joined back to an edit seen in recentchange
//
//   npm run phase0:tags -- --minutes 30

import { streamEvents } from '../stream/sse.ts';
import { addedTags, isClassifiable, RecentChange, REVERT_TAGS, TagsChange } from './events.ts';
import { args, log, runFor, stamp, STREAM_BASE, writeJson } from './cli.ts';
import { round, summarise } from './stats.ts';

const opts = args({
  minutes: { type: 'string', default: '30' },
  wiki: { type: 'string', default: 'enwiki' },
});
const minutes = Number(opts.minutes);
const wiki = String(opts.wiki);
const startedAt = new Date();

let rcTotal = 0;
let rcWithTagsField = 0;
const rcFocusRevIds = new Set<number>();
let tcTotal = 0;
let tcFocus = 0;
let parseFailures = 0;
const added = Object.fromEntries(REVERT_TAGS.map((t) => [t, 0])) as Record<string, number>;
const addedInMainspace = Object.fromEntries(REVERT_TAGS.map((t) => [t, 0])) as Record<string, number>;
const revertDelaysSec: number[] = [];
let revertedJoinable = 0;
const examples: unknown[] = [];

const ctl = runFor(minutes);
log(`probing tags for ${minutes} min on ${wiki}`);

for await (const ev of streamEvents(`${STREAM_BASE}recentchange,mediawiki.revision-tags-change`, {
  signal: ctl.signal,
  onDisconnect: (r) => log(`disconnect: ${r}`),
})) {
  let raw: unknown;
  try {
    raw = JSON.parse(ev.data);
  } catch {
    parseFailures++;
    continue;
  }
  const stream = (raw as { meta?: { stream?: string } }).meta?.stream;

  if (stream === 'mediawiki.recentchange') {
    const p = RecentChange.safeParse(raw);
    if (!p.success) {
      parseFailures++;
      continue;
    }
    rcTotal++;
    if ('tags' in p.data) rcWithTagsField++;
    if (p.data.wiki === wiki && isClassifiable(p.data) && p.data.revision) rcFocusRevIds.add(p.data.revision.new);
  } else if (stream === 'mediawiki.revision-tags-change') {
    const p = TagsChange.safeParse(raw);
    if (!p.success) {
      parseFailures++;
      continue;
    }
    tcTotal++;
    const tc = p.data;
    if (tc.database !== wiki) continue;
    tcFocus++;
    for (const tag of addedTags(tc)) {
      if (!(tag in added)) continue;
      added[tag]!++;
      if (tc.page_namespace === 0) addedInMainspace[tag]!++;
      if (tag === 'mw-reverted') {
        revertDelaysSec.push((Date.parse(tc.meta.dt) - Date.parse(tc.rev_timestamp)) / 1000);
        if (rcFocusRevIds.has(tc.rev_id)) revertedJoinable++;
        if (examples.length < 5) examples.push(tc);
      }
    }
  }
}

const reverted = added['mw-reverted'] ?? 0;
const report = {
  wiki,
  startedAt: startedAt.toISOString(),
  endedAt: new Date().toISOString(),
  parseFailures,
  recentchange: {
    events: rcTotal,
    withTagsField: rcWithTagsField,
    verdict: rcWithTagsField === 0 ? 'recentchange carries no tags; labels must come from elsewhere' : 'recentchange carries tags',
  },
  revisionTagsChange: {
    events: tcTotal,
    focusWikiEvents: tcFocus,
    revertTagsAdded: added,
    revertTagsAddedMainspace: addedInMainspace,
    // Seconds from the original edit to mw-reverted being applied. Only reverts that
    // happened during the probe are seen, so this is biased toward fast reverts.
    mwRevertedDelaySeconds: summarise(revertDelaysSec),
    // Reverted revisions whose original edit was also seen in recentchange during this probe.
    // Low early in a short probe because most reverted edits predate it.
    mwRevertedJoinableToRecentchange: { count: revertedJoinable, of: reverted, share: reverted ? round(revertedJoinable / reverted) : null },
  },
  examples,
};

const out = `results/phase0/tags-probe-${stamp(startedAt)}.json`;
writeJson(out, report);
log(`wrote ${out}`);
log(`${report.recentchange.verdict}; mw-reverted added ${reverted} times on ${wiki}`);
