// Phase 3, step 2: score each filter policy on committed edit tables.
//
// For each table and policy: N = share of edits the filter drops, M = share of later-reverted
// edits among them (the revert recall the cost saving gives up). Also an early read on true
// vandalism, from the Phase 0 sample rows that fall in a table.
//
//   npm run phase3:evaluate -- --tables dev-2026-09-30T1200Z,heldout-2026-09-30T0400Z

import { readFileSync } from 'node:fs';
import { decide, POLICIES, type Policy, type UserSnapshot } from '../filter/rules.ts';
import type { UserClass } from '../phase0/events.ts';
import { args, log, writeJson } from '../phase0/cli.ts';
import { parseCsv } from '../phase0/csv.ts';
import { round, wilson } from '../phase0/stats.ts';

const opts = args({
  tables: { type: 'string' },
  sample: { type: 'string', default: 'labels/sample.csv' },
  out: { type: 'string', default: 'results/phase3/filter-eval.json' },
});
if (!opts.tables) throw new Error('--tables is required (comma-separated table names)');

interface EditRow {
  revId: number;
  timestamp: number;
  userClass: UserClass;
  user: UserSnapshot | undefined;
  reverted: boolean | undefined;
}

function load(name: string): EditRow[] {
  return parseCsv(readFileSync(`results/phase3/edits-${name}.csv`, 'utf8')).map((r) => ({
    revId: Number(r.rev_id),
    timestamp: Number(r.timestamp),
    userClass: r.user_class as UserClass,
    user:
      r.editcount === '' || r.editcount === undefined
        ? undefined
        : { registration: r.registration === 'null' ? null : r.registration!, editcount: Number(r.editcount) },
    reverted: r.reverted === 'gone' ? undefined : r.reverted === '1',
  }));
}

function score(rows: readonly EditRow[], policy: Policy) {
  const decided = rows.map((r) => ({ r, d: decide(r, r.user, policy) }));
  const dropped = decided.filter((x) => !x.d.keep);
  const labelled = decided.filter((x) => x.r.reverted !== undefined);
  const reverted = labelled.filter((x) => x.r.reverted);
  const revertedDropped = reverted.filter((x) => !x.d.keep);
  const nonBot = decided.filter((x) => x.r.userClass !== 'bot');
  return {
    edits: rows.length,
    dropped: dropped.length,
    byRule: {
      bot: dropped.filter((x) => x.d.rule === 'bot').length,
      trustedAccount: dropped.filter((x) => x.d.rule === 'trusted-account').length,
    },
    // N: volume the filter removes, all edits and non-bot only (bot share swings by hour, D8).
    volumeRemoved: round(dropped.length / rows.length),
    volumeRemovedNonBot: round(nonBot.filter((x) => !x.d.keep).length / nonBot.length),
    // M: later-reverted edits the filter throws away.
    revertedLost: wilson(revertedDropped.length, reverted.length),
    // Revert rate in what reaches the models, against the rate before filtering.
    revertRateBefore: round(reverted.length / labelled.length),
    revertRateAfter: round((reverted.length - revertedDropped.length) / labelled.filter((x) => x.d.keep).length),
    revertedLostBy: {
      bot: revertedDropped.filter((x) => x.d.rule === 'bot').length,
      trustedAccount: revertedDropped.filter((x) => x.d.rule === 'trusted-account').length,
    },
  };
}

const names = String(opts.tables).split(',');
const tables = Object.fromEntries(names.map((n) => [n, load(n)]));

// The Phase 0 sample: AI-labelled vandalism (D9), joined by rev_id to any table that holds it.
const sample = parseCsv(readFileSync(String(opts.sample), 'utf8'));
const byRev = new Map(Object.values(tables).flat().map((r) => [r.revId, r]));
const vandalism = sample.filter((r) => r.vandalism === 'y').map((r) => byRev.get(Number(r.rev_id)));
const found = vandalism.filter((r): r is EditRow => r !== undefined);

const report = {
  evaluatedAt: new Date().toISOString(),
  target: 'reverted within 72h (DECISIONS.md D9)',
  tables: Object.fromEntries(
    names.map((n) => [n, Object.fromEntries(POLICIES.map((p) => [p, score(tables[n]!, p)]))]),
  ),
  phase0VandalismRows: {
    note: 'Early read only: AI-labelled (D9), and these rows lie in the dev table.',
    vandalismRows: vandalism.length,
    foundInTables: found.length,
    droppedByPolicy: Object.fromEntries(POLICIES.map((p) => [p, found.filter((r) => !decide(r, r.user, p).keep).length])),
  },
};
writeJson(String(opts.out), report);

for (const n of names) {
  for (const p of POLICIES) {
    const s = report.tables[n]![p]!;
    log(
      `${n.padEnd(28)} ${p.padEnd(18)} removes ${(s.volumeRemoved * 100).toFixed(1)}% of edits ` +
        `(${(s.volumeRemovedNonBot * 100).toFixed(1)}% of non-bot); loses ${(s.revertedLost.rate * 100).toFixed(1)}% ` +
        `[${(s.revertedLost.low * 100).toFixed(1)}, ${(s.revertedLost.high * 100).toFixed(1)}] of reverted`,
    );
  }
}
log(`phase 0 vandalism rows dropped: ${JSON.stringify(report.phase0VandalismRows.droppedByPolicy)} of ${found.length}`);
log(`wrote ${opts.out}`);
