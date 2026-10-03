// Phase 4 validation: does the stream labeller agree with the Action API?
//
// Feeds the labeller a captured hour of edits merged (by event time) with the replayed
// revision-tags-change stream for the following three days, then compares every label with the
// Phase 3 table for the same hour, whose labels came from the Action API ~80h after the edits.
//
//   npm run phase4:validate -- --capture data/phase0/capture-us-morning-2026-09-30T1200Z.jsonl \
//     --tags data/phase4/tags-enwiki-….jsonl --table dev-2026-09-30T1200Z

import { readFileSync } from 'node:fs';
import { args, log, writeJson } from '../phase0/cli.ts';
import { parseCsv } from '../phase0/csv.ts';
import { addedTags, TagsChange } from '../phase0/events.ts';
import { round, wilson } from '../phase0/stats.ts';
import { DEFAULT_LABELLER } from '../labels/labeller.ts';
import { labelReplay, REPLAY_LABELLER } from '../labels/replay-labels.ts';

const opts = args({
  capture: { type: 'string' },
  tags: { type: 'string' },
  table: { type: 'string' },
  'outage-gap-minutes': { type: 'string', default: '30' },
});
if (!opts.capture || !opts.tags || !opts.table) throw new Error('--capture, --tags and --table are required');

// Every mw-reverted addition for a captured edit, at any delay: explains disagreements later.
const captureRevs = new Set(
  readFileSync(String(opts.capture), 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => (JSON.parse(line) as { revision?: { new?: number } }).revision?.new)
    .filter((r): r is number => r !== undefined),
);
const revertTimes = new Map<number, number[]>();
let tagEvents = 0;
let tagsEndMs = -Infinity;
const topics = new Map<string, number>();
const labellerOpts = { ...REPLAY_LABELLER, outageGapMs: Number(opts['outage-gap-minutes']) * 60_000 };
const { labels, labeller } = await labelReplay(String(opts.capture), String(opts.tags), labellerOpts, (e) => {
  if (e.source !== 'tags') return;
  tagEvents++;
  if (e.dt > tagsEndMs) tagsEndMs = e.dt;
  const p = TagsChange.safeParse(JSON.parse(e.line));
  if (!p.success) return;
  const t = String((p.data.meta as { topic?: unknown }).topic ?? '?');
  topics.set(t, (topics.get(t) ?? 0) + 1);
  if (captureRevs.has(p.data.rev_id) && addedTags(p.data).includes('mw-reverted')) {
    const list = revertTimes.get(p.data.rev_id) ?? [];
    list.push(e.dt);
    revertTimes.set(p.data.rev_id, list);
  }
});

// Compare with the Action API table.
const table = parseCsv(readFileSync(`results/phase3/edits-${opts.table}.csv`, 'utf8'));
const apiAskedMs = Date.parse(
  (JSON.parse(readFileSync(`results/phase3/edits-${opts.table}.meta.json`, 'utf8')) as { queriedAt: string }).queriedAt,
);
const confusion: Record<string, number> = {};
const disagreements: { revId: number; api: string; stream: string; explanation: string }[] = [];
let unlabelled = 0;
for (const row of table) {
  const revId = Number(row.rev_id);
  const l = labels.get(revId);
  if (!l) {
    unlabelled++;
    continue;
  }
  const api = row.reverted === '1' ? 'reverted' : row.reverted === '0' ? 'not-reverted' : 'gone';
  const key = `api:${api} / stream:${l.label}`;
  confusion[key] = (confusion[key] ?? 0) + 1;
  if (api === 'gone' || api === l.label) continue;
  const times = revertTimes.get(revId) ?? [];
  const editMs = Date.parse(l.editTime);
  const explanation =
    api === 'reverted' && times.length === 0
      ? tagsEndMs < apiAskedMs && tagsEndMs >= editMs + DEFAULT_LABELLER.windowMs
        ? 'no revert tag before the replay ended (past the window); the API was asked later'
        : 'revert tag never seen in the stream'
      : api === 'reverted' && times.every((t) => t > editMs + DEFAULT_LABELLER.windowMs)
        ? `reverted after the window (${round((Math.min(...times) - editMs) / 3_600_000)}h), before the API was asked`
        : api === 'not-reverted' && l.label === 'reverted'
          ? 'stream saw mw-reverted added; the API no longer shows it (tag removed later?)'
          : 'other';
  disagreements.push({ revId, api, stream: l.label, explanation });
}
const compared = table.length - unlabelled - (confusion['api:gone / stream:not-reverted'] ?? 0) - (confusion['api:gone / stream:reverted'] ?? 0);
const agree = (confusion['api:reverted / stream:reverted'] ?? 0) + (confusion['api:not-reverted / stream:not-reverted'] ?? 0);
const why: Record<string, number> = {};
for (const d of disagreements) why[d.explanation.replace(/\(.*\)/, '(…)')] = (why[d.explanation.replace(/\(.*\)/, '(…)')] ?? 0) + 1;

const noise = JSON.parse(readFileSync('results/phase0/label-noise.json', 'utf8')) as Record<string, unknown>;
const report = {
  validatedAt: new Date().toISOString(),
  table: opts.table,
  inputs: {
    capture: opts.capture,
    tags: opts.tags,
    tagEvents,
    tagTopics: Object.fromEntries(topics),
    tagsEnd: new Date(tagsEndMs).toISOString(),
    apiAsked: new Date(apiAskedMs).toISOString(),
  },
  labeller: { ...labellerOpts, stats: labeller.stats, stillPending: labeller.pending },
  tableRows: table.length,
  labelledByStream: table.length - unlabelled,
  // Rows the API could not see (deleted / suppressed revisions) are excluded from agreement.
  compared,
  agreement: wilson(agree, compared),
  confusion,
  disagreementsByCause: why,
  disagreements,
  // Carried beside every label figure (SPEC Phase 4): how far "reverted" is from "vandalism".
  labelNoise: {
    source: 'results/phase0/label-noise.json',
    labeller: noise.labeller,
    revertedNotVandalism: noise.revertedNotVandalism,
    keptButVandalism: noise.keptButVandalism,
  },
};
writeJson(`results/phase4/validation-${opts.table}.json`, report);
log(`${opts.table}: ${agree}/${compared} agree (${(report.agreement.rate * 100).toFixed(2)}%); ${JSON.stringify(why)}`);
log(`confusion ${JSON.stringify(confusion)}; outages ${labeller.stats.outages.length}; unlabelled ${unlabelled}`);
