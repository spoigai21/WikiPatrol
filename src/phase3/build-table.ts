// Phase 3, step 1: turn a raw capture into a committed per-edit table the filter is scored on.
//
// One row per classifiable enwiki edit (main namespace, edit or page creation, bots included):
// the filter's inputs plus the revert label. No usernames. The table is the reproducible record;
// captures age out of the stream's replay window, the table does not.
//
//   npm run phase3:table -- --capture data/phase0/capture-us-morning-2026-09-30T1200Z.jsonl --name dev-2026-09-30T1200Z

import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { classifyUser, isClassifiable, RecentChange, type UserClass } from '../phase0/events.ts';
import { args, log, writeJson } from '../phase0/cli.ts';
import { toCsv } from '../phase0/csv.ts';
import { ENWIKI_API, fetchRevisionTags, fetchUsers } from '../labels/action-api.ts';
import { round } from '../phase0/stats.ts';

const opts = args({
  capture: { type: 'string' },
  name: { type: 'string' },
  wiki: { type: 'string', default: 'enwiki' },
  // The label window (DECISIONS.md D4). Younger edits have not had their chance to be reverted.
  'min-age-hours': { type: 'string', default: '72' },
  api: { type: 'string', default: ENWIKI_API },
});
if (!opts.capture || !opts.name) throw new Error('--capture and --name are required');
const minAgeHours = Number(opts['min-age-hours']);
const api = String(opts.api);

interface Row {
  revId: number;
  timestamp: number;
  userClass: UserClass;
  user: string;
}

const rows = new Map<number, Row>();
for (const line of readFileSync(String(opts.capture), 'utf8').split('\n')) {
  if (!line) continue;
  const p = RecentChange.safeParse(JSON.parse(line));
  if (!p.success) continue;
  const rc = p.data;
  if (rc.wiki !== opts.wiki || !isClassifiable(rc) || !rc.revision || rc.timestamp === undefined) continue;
  rows.set(rc.revision.new, { revId: rc.revision.new, timestamp: rc.timestamp, userClass: classifyUser(rc.user, rc.bot), user: rc.user ?? '' });
}
const all = [...rows.values()].sort((a, b) => a.revId - b.revId);
if (all.length === 0) throw new Error('no classifiable edits in capture');

const queriedAt = Date.now();
const ages = all.map((r) => (queriedAt / 1000 - r.timestamp) / 3600);
const youngest = Math.min(...ages);
if (youngest < minAgeHours) {
  throw new Error(`youngest edit is ${youngest.toFixed(1)}h old; the ${minAgeHours}h label window has not closed`);
}

log(`${all.length} classifiable edits; fetching revert tags`);
const tags = await fetchRevisionTags(all.map((r) => r.revId), api);
const registered = [...new Set(all.filter((r) => r.userClass === 'registered').map((r) => r.user))];
log(`fetching ${registered.length} registered accounts`);
const users = await fetchUsers(registered, api);

const label = (revId: number) => {
  const t = tags.get(revId);
  return t === undefined ? 'gone' : t.includes('mw-reverted') ? '1' : '0';
};

mkdirSync('results/phase3', { recursive: true });
const tablePath = `results/phase3/edits-${opts.name}.csv`;
writeFileSync(
  tablePath,
  toCsv(
    ['rev_id', 'timestamp', 'user_class', 'registration', 'editcount', 'reverted'],
    all.map((r) => {
      const u = r.userClass === 'registered' ? users.get(r.user) : undefined;
      return [r.revId, r.timestamp, r.userClass, u ? (u.registration ?? 'null') : '', u ? u.editcount : '', label(r.revId)];
    }),
  ),
);

const counts = { reverted: 0, kept: 0, gone: 0 };
for (const r of all) {
  const l = label(r.revId);
  if (l === '1') counts.reverted++;
  else if (l === '0') counts.kept++;
  else counts.gone++;
}
writeJson(`results/phase3/edits-${opts.name}.meta.json`, {
  name: opts.name,
  capture: opts.capture,
  editTimeRange: [new Date(all[0]!.timestamp * 1000).toISOString(), new Date(Math.max(...all.map((r) => r.timestamp)) * 1000).toISOString()],
  queriedAt: new Date(queriedAt).toISOString(),
  // Tags and edit counts are read once, now; revert status is "reverted by query time".
  ageAtQueryHours: { min: round(youngest), max: round(Math.max(...ages)) },
  edits: all.length,
  labels: counts,
  registeredAccounts: { asked: registered.length, found: users.size },
  columns: {
    registration: "account creation (ISO); 'null' = predates the field; empty = not a registered account",
    editcount: 'edit count at queriedAt, not at the edit',
    reverted: "1 = tagged mw-reverted; 0 = not; gone = revision deleted or suppressed, excluded from rates",
  },
});
log(`wrote ${tablePath}: ${JSON.stringify(counts)}`);
