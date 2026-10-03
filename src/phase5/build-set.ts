// Phase 5: freeze an evaluation set — every model and baseline is scored on exactly these bytes.
//
// From a replayed hour: label every classifiable edit (labeller, D11), apply the default filter
// (D10), draw a seeded sample of the edits that survive it, and fetch each sampled edit's diff.
// Written as gzipped JSONL plus a manifest carrying the SHA-256 of the uncompressed content, so
// the set survives the stream's replay window, a cluster rebuild, and any later page edits.
//
//   npm run phase5:set -- --name sealed --capture data/phase0/capture-sealed-2026-09-30T1000Z.jsonl \
//     --tags data/phase4/tags-….jsonl --sample 1000
//   npm run phase5:set -- --name sealed --repair      (re-fetch diffs that failed in transit)

import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { gunzipSync, gzipSync } from 'node:zlib';
import { args, log, writeJson } from '../phase0/cli.ts';
import { classifyUser, isClassifiable, RecentChange, type UserClass } from '../phase0/events.ts';
import { mulberry32, round, shuffled } from '../phase0/stats.ts';
import { accountAgeDays, decide, DEFAULT_POLICY } from '../filter/rules.ts';
import { fetchDiff, fetchUsers } from '../labels/action-api.ts';
import type { LabelValue } from '../labels/labeller.ts';
import { labelReplay } from '../labels/replay-labels.ts';
import { renderDiff } from './diff.ts';

export const DIFF_MAX_CHARS = 3000;

/** One edit in an evaluation set. Everything a model may see, plus the label it must not. */
export interface SetEdit {
  revId: number;
  title: string;
  editTime: string;
  isNew: boolean;
  minor: boolean;
  userClass: UserClass;
  /** At the time of the edit; null when not a registered account. */
  accountAgeDays: number | null;
  editcount: number | null;
  sizeDelta: number;
  comment: string;
  diff: string;
  diffTruncated: boolean;
  /** Set when the API says the content is gone (deleted, hidden); `diff` is then empty. */
  diffUnavailable?: string;
  label: LabelValue;
}

export const setPath = (name: string) => `results/phase5/sets/${name}.jsonl.gz`;
export const manifestPath = (name: string) => `results/phase5/sets/${name}.manifest.json`;

/** The diff fields of a SetEdit, fetched now. Transport failures throw; they are never frozen. */
async function diffFields(revId: number, isNew: boolean): Promise<Pick<SetEdit, 'diff' | 'diffTruncated' | 'diffUnavailable'>> {
  const d = await fetchDiff(revId, isNew);
  if (d.kind === 'unavailable') return { diff: '', diffTruncated: false, diffUnavailable: d.reason };
  if (d.kind === 'diff') {
    const r = renderDiff(d.html, DIFF_MAX_CHARS);
    return { diff: r.text, diffTruncated: r.truncated };
  }
  return d.text.length > DIFF_MAX_CHARS
    ? { diff: d.text.slice(0, DIFF_MAX_CHARS) + '\n[page text truncated]', diffTruncated: true }
    : { diff: d.text, diffTruncated: false };
}

function writeSet(name: string, rows: readonly SetEdit[], manifest: Record<string, unknown>): string {
  const jsonl = rows.map((r) => JSON.stringify(r)).join('\n') + '\n';
  const sha256 = createHash('sha256').update(jsonl).digest('hex');
  mkdirSync('results/phase5/sets', { recursive: true });
  writeFileSync(setPath(name), gzipSync(jsonl, { level: 9 }));
  // Checksum of the uncompressed JSONL, so it does not depend on the gzip implementation.
  writeJson(manifestPath(name), { ...manifest, diffUnavailable: rows.filter((r) => r.diffUnavailable).length, sha256 });
  return sha256;
}

/**
 * Re-fetch diffs that an earlier build froze as unavailable because of a transport failure (an
 * HTTP error, not the API saying the content is gone). Every other byte of the set is kept, so
 * the sample does not change. Refused once any run exists on the set.
 */
async function repair(name: string): Promise<void> {
  const runs = existsSync(`results/phase5/runs/${name}`) ? readdirSync(`results/phase5/runs/${name}`) : [];
  if (runs.length) throw new Error(`refusing to repair "${name}": ${runs.length} run(s) already scored on it`);
  const rows = gunzipSync(readFileSync(setPath(name))).toString('utf8').trimEnd().split('\n').map((l) => JSON.parse(l) as SetEdit);
  const manifest = JSON.parse(readFileSync(manifestPath(name), 'utf8')) as Record<string, unknown>;
  const broken = rows.filter((r) => r.diffUnavailable?.startsWith('Action API HTTP'));
  log(`${broken.length} of ${rows.length} rows failed in transit; re-fetching`);
  for (const r of broken) {
    delete r.diffUnavailable;
    Object.assign(r, await diffFields(r.revId, r.isNew));
    await new Promise((res) => setTimeout(res, 100));
  }
  const repairs = [...((manifest.repairs as unknown[]) ?? []), { at: new Date().toISOString(), rows: broken.length, previousSha256: manifest.sha256 }];
  const sha256 = writeSet(name, rows, { ...manifest, repairs });
  log(`repaired ${setPath(name)}; sha256 ${sha256.slice(0, 16)}…`);
}

if (import.meta.main) {
  const opts = args({
    name: { type: 'string' },
    capture: { type: 'string' },
    tags: { type: 'string' },
    sample: { type: 'string' },
    seed: { type: 'string', default: '20261003' },
    wiki: { type: 'string', default: 'enwiki' },
    repair: { type: 'boolean', default: false },
  });
  if (!opts.name) throw new Error('--name is required');
  if (opts.repair) {
    await repair(String(opts.name));
    process.exit(0);
  }
  if (!opts.capture || !opts.tags || !opts.sample) throw new Error('--capture, --tags and --sample are required');
  const sampleSize = Number(opts.sample);
  const seed = Number(opts.seed);

  log('labelling the hour from the replayed log');
  const { labels } = await labelReplay(String(opts.capture), String(opts.tags));

  const edits = new Map<number, { rc: RecentChange; userClass: UserClass }>();
  for (const line of readFileSync(String(opts.capture), 'utf8').split('\n')) {
    if (!line) continue;
    const p = RecentChange.safeParse(JSON.parse(line));
    if (!p.success) continue;
    const rc = p.data;
    if (rc.wiki !== opts.wiki || !isClassifiable(rc) || !rc.revision || rc.timestamp === undefined) continue;
    edits.set(rc.revision.new, { rc, userClass: classifyUser(rc.user, rc.bot) });
  }
  const unlabelled = [...edits.keys()].filter((r) => !labels.has(r));
  if (unlabelled.length) throw new Error(`${unlabelled.length} edits have no label: the tags replay does not cover their window`);

  const registered = [...new Set([...edits.values()].filter((e) => e.userClass === 'registered').map((e) => e.rc.user ?? ''))];
  log(`${edits.size} edits; fetching ${registered.length} accounts`);
  const users = await fetchUsers(registered);

  const byRule = { bot: 0, 'trusted-account': 0, kept: 0 };
  const survivors: number[] = [];
  for (const [revId, e] of [...edits].sort((a, b) => a[0] - b[0])) {
    const d = decide({ userClass: e.userClass, timestamp: e.rc.timestamp! }, users.get(e.rc.user ?? ''), DEFAULT_POLICY);
    byRule[d.rule]++;
    if (d.keep) survivors.push(revId);
  }
  const sample = shuffled(survivors, mulberry32(seed)).slice(0, sampleSize);
  log(`${survivors.length} survive the filter (${JSON.stringify(byRule)}); fetching ${sample.length} diffs`);

  const rows: SetEdit[] = [];
  for (const [i, revId] of sample.entries()) {
    const { rc, userClass } = edits.get(revId)!;
    const user = userClass === 'registered' ? users.get(rc.user ?? '') : undefined;
    const isNew = rc.type === 'new';
    rows.push({
      revId,
      title: rc.title ?? '',
      editTime: new Date(rc.timestamp! * 1000).toISOString(),
      isNew,
      minor: rc.minor ?? false,
      userClass,
      accountAgeDays: user ? round(Math.min(accountAgeDays(user, rc.timestamp!), 99_999)) : null,
      editcount: user ? user.editcount : null,
      sizeDelta: (rc.length?.new ?? 0) - (rc.length?.old ?? 0),
      comment: rc.comment ?? '',
      ...(await diffFields(revId, isNew)),
      label: labels.get(revId)!.label,
    });
    if ((i + 1) % 100 === 0) log(`${i + 1}/${sample.length} diffs`);
    await new Promise((r) => setTimeout(r, 100));
  }

  const sha256 = writeSet(String(opts.name), rows, {
    name: opts.name,
    builtAt: new Date().toISOString(),
    capture: opts.capture,
    tags: opts.tags,
    editTimeRange: [rows.map((r) => r.editTime).sort()[0], rows.map((r) => r.editTime).sort().at(-1)],
    seed,
    filterPolicy: DEFAULT_POLICY,
    diffMaxChars: DIFF_MAX_CHARS,
    classifiableEdits: edits.size,
    filter: byRule,
    sampled: rows.length,
  });
  log(`wrote ${setPath(String(opts.name))} (${rows.length} edits, sha256 ${sha256.slice(0, 16)}…)`);
}
