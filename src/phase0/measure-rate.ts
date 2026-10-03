// Phase 0, step 1: how fast is the feed, really?
//
// Counts events by event time (meta.dt), not arrival time, so a reconnect that
// replays a backlog does not show up as a fake burst. Also captures raw enwiki
// edit/new events to data/ so the label-noise sample can be drawn from them.
//
//   npm run phase0:rate -- --minutes 60 --label us-daytime
//   npm run phase0:rate -- --minutes 60 --label us-overnight --since 2026-10-03T09:00:00Z
//
// --since replays a past hour from the stream's history (7-31 days, src/stream/replay.ts).
// Because counting is by event time, a replayed hour measures the same thing as a live one;
// it ends when the events pass since + minutes, not when the wall clock does.

import { createWriteStream, mkdirSync } from 'node:fs';
import { streamEvents } from '../stream/sse.ts';
import { replayWindow } from '../stream/replay.ts';
import { classifyUser, isClassifiable, RecentChange, type UserClass } from './events.ts';
import { args, log, runFor, stamp, STREAM_BASE, writeJson } from './cli.ts';
import { round, summarise } from './stats.ts';

const opts = args({
  minutes: { type: 'string', default: '60' },
  label: { type: 'string', default: 'adhoc' },
  wiki: { type: 'string', default: 'enwiki' },
  since: { type: 'string' },
});
const minutes = Number(opts.minutes);
const focusWiki = String(opts.wiki);
const startedAt = new Date();
const since = opts.since === undefined ? undefined : new Date(String(opts.since));
if (since && Number.isNaN(since.getTime())) throw new Error(`--since is not a date: ${opts.since}`);
const runId = `${opts.label}-${stamp(since ?? startedAt)}`;

mkdirSync('data/phase0', { recursive: true });
const capturePath = `data/phase0/capture-${runId}.jsonl`;
const capture = createWriteStream(capturePath);

const byType = new Map<string, number>();
const byWiki = new Map<string, number>();
const focusByUser = new Map<UserClass, number>();
const focusClassifiableByUser = new Map<UserClass, number>();
const perSecondAll = new Map<number, number>();
const perSecondFocus = new Map<number, number>();
const perSecondClassifiable = new Map<number, number>();
const seenIds = new Set<string>();
let total = 0;
let duplicates = 0;
let parseFailures = 0;
let connects = 0;
const disconnects: string[] = [];

const bump = <K>(m: Map<K, number>, k: K) => m.set(k, (m.get(k) ?? 0) + 1);

// A replay runs faster than real time; the wall-clock limit is only a safety net.
const ctl = runFor(since ? minutes * 2 : minutes);
log(`measuring ${minutes} min${since ? ` replayed from ${since.toISOString()}` : ''}, run ${runId}; raw ${focusWiki} edits -> ${capturePath}`);

const streamOpts = {
  signal: ctl.signal,
  onConnect: (attempt: number, last: string | undefined) => {
    connects++;
    log(`connect #${connects} (failures ${attempt})${last ? ' resuming from Last-Event-ID' : ''}`);
  },
  onDisconnect: (reason: string) => {
    disconnects.push(`${new Date().toISOString()} ${reason}`);
    log(`disconnect: ${reason}`);
  },
};
const events = since
  ? (async function* () {
      const until = new Date(since.getTime() + minutes * 60_000);
      for await (const r of replayWindow(`${STREAM_BASE}recentchange`, since, until, { ...streamOpts, onUnparseable: () => parseFailures++ })) yield r.ev;
    })()
  : streamEvents(`${STREAM_BASE}recentchange`, streamOpts);

for await (const ev of events) {
  let rc: RecentChange;
  try {
    const parsed = RecentChange.safeParse(JSON.parse(ev.data));
    if (!parsed.success) {
      parseFailures++;
      continue;
    }
    rc = parsed.data;
  } catch {
    parseFailures++;
    continue;
  }
  if (seenIds.has(rc.meta.id)) {
    duplicates++;
    continue;
  }
  seenIds.add(rc.meta.id);

  total++;
  const sec = Math.floor(Date.parse(rc.meta.dt) / 1000);
  bump(perSecondAll, sec);
  bump(byType, rc.type);
  bump(byWiki, rc.wiki);

  if (rc.wiki === focusWiki) {
    bump(perSecondFocus, sec);
    if (rc.type === 'edit' || rc.type === 'new') {
      const cls = classifyUser(rc.user, rc.bot);
      bump(focusByUser, cls);
      capture.write(ev.data + '\n');
      if (isClassifiable(rc)) {
        bump(perSecondClassifiable, sec);
        bump(focusClassifiableByUser, cls);
      }
    }
  }
  if (total % 5000 === 0) log(`${total} events`);
}

ctl.abort();
capture.end();
const endedAt = new Date();

// Fill empty seconds with zero, trimming the first and last partial seconds.
function rateSeries(m: Map<number, number>, lo: number, hi: number): number[] {
  const out: number[] = [];
  for (let s = lo + 1; s < hi; s++) out.push(m.get(s) ?? 0);
  return out;
}
const secs = [...perSecondAll.keys()];
const lo = Math.min(...secs);
const hi = Math.max(...secs);
const span = Math.max(1, hi - lo - 1);

function perMinute(m: Map<number, number>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [s, n] of [...m].sort((a, b) => a[0] - b[0])) {
    const k = new Date(s * 1000).toISOString().slice(0, 16) + 'Z';
    out[k] = (out[k] ?? 0) + n;
  }
  return out;
}

const share = (m: Map<string, number>) => {
  const n = [...m.values()].reduce((a, b) => a + b, 0);
  return Object.fromEntries([...m].sort((a, b) => b[1] - a[1]).map(([k, v]) => [k, { count: v, share: round(v / n) }]));
};

const report = {
  runId,
  label: opts.label,
  focusWiki,
  startedAt: startedAt.toISOString(),
  endedAt: endedAt.toISOString(),
  replayedFrom: since?.toISOString() ?? null,
  eventTimeSpanSeconds: span,
  connection: { connects, disconnects, duplicatesDropped: duplicates, parseFailures },
  totals: {
    allWikis: total,
    focusWikiAllTypes: [...perSecondFocus.values()].reduce((a, b) => a + b, 0),
    focusWikiEdits: [...focusByUser.values()].reduce((a, b) => a + b, 0),
    focusWikiClassifiable: [...perSecondClassifiable.values()].reduce((a, b) => a + b, 0),
  },
  // Events per second, by event time. "classifiable" = focus wiki, main namespace, edit or new page.
  ratePerSecond: {
    allWikis: summarise(rateSeries(perSecondAll, lo, hi)),
    focusWikiAllTypes: summarise(rateSeries(perSecondFocus, lo, hi)),
    focusWikiClassifiable: summarise(rateSeries(perSecondClassifiable, lo, hi)),
  },
  byType: share(byType),
  topWikis: Object.fromEntries(Object.entries(share(byWiki)).slice(0, 25)),
  focusWikiEditsByUser: share(focusByUser as Map<string, number>),
  focusWikiClassifiableByUser: share(focusClassifiableByUser as Map<string, number>),
  perMinute: {
    allWikis: perMinute(perSecondAll),
    focusWikiClassifiable: perMinute(perSecondClassifiable),
  },
  capture: capturePath,
};

const out = `results/phase0/rate-${runId}.json`;
writeJson(out, report);
log(`wrote ${out}`);
log(
  `all wikis ${report.ratePerSecond.allWikis.mean}/s; ${focusWiki} classifiable ` +
    `${report.ratePerSecond.focusWikiClassifiable.mean}/s (p95 ${report.ratePerSecond.focusWikiClassifiable.p95})`,
);
