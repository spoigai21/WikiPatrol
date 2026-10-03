// Phase 4 validation input: replay `mediawiki.revision-tags-change` for a past window, keeping one
// wiki's events, so the stream labeller can be checked against the Action API without waiting
// 72 hours for a live run.
//
//   npm run phase4:backfill-tags -- --since 2026-09-30T04:00:00Z --until 2026-10-03T13:20:00Z

import { createWriteStream, mkdirSync } from 'node:fs';
import { args, log, stamp, STREAM_BASE } from '../phase0/cli.ts';
import { replayWindow } from '../stream/replay.ts';

const opts = args({
  since: { type: 'string' },
  until: { type: 'string' },
  wiki: { type: 'string', default: 'enwiki' },
});
if (!opts.since || !opts.until) throw new Error('--since and --until are required');
const since = new Date(String(opts.since));
const until = new Date(String(opts.until));

mkdirSync('data/phase4', { recursive: true });
const path = `data/phase4/tags-${opts.wiki}-${stamp(since)}-${stamp(until)}.jsonl`;
const out = createWriteStream(path);
let total = 0;
let kept = 0;
let unparseable = 0;
const ctl = new AbortController();
process.once('SIGINT', () => ctl.abort());

for await (const { ev } of replayWindow(`${STREAM_BASE}mediawiki.revision-tags-change`, since, until, {
  signal: ctl.signal,
  onUnparseable: () => unparseable++,
  onDisconnect: (r) => log(`disconnect: ${r}`),
})) {
  total++;
  // Cheap prefilter before parsing: the database name appears verbatim in the payload.
  if (ev.data.includes(`"database":"${opts.wiki}"`)) {
    out.write(ev.data + '\n');
    kept++;
  }
  if (total % 100_000 === 0) log(`${total} events, ${kept} ${opts.wiki}`);
}
out.end();
log(`done: ${total} events in window, ${kept} ${opts.wiki} -> ${path}; ${unparseable} unparseable`);
