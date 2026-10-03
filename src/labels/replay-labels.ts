// Label a replayed hour offline: merge a recentchange capture with a revision-tags-change replay
// by event time and run the same Labeller the live service runs (validated against the Action
// API in DECISIONS.md D11).

import { createReadStream, readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { DEFAULT_LABELLER, Labeller, type Label, type LabellerOptions } from './labeller.ts';

export interface MergedEvent {
  line: string;
  /** meta.dt in epoch ms (NaN if unparseable). */
  dt: number;
  source: 'capture' | 'tags';
}

const dtOf = (line: string) => {
  try {
    return Date.parse((JSON.parse(line) as { meta: { dt: string } }).meta.dt);
  } catch {
    return NaN;
  }
};

/**
 * Yields the capture (small; loaded and sorted) merged into the tags file (large; streamed, already
 * in event-time order), each capture event placed before the first tag event later than it.
 */
export async function* mergeByTime(capturePath: string, tagsPath: string): AsyncGenerator<MergedEvent> {
  const captured = readFileSync(capturePath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => ({ line, dt: dtOf(line) }))
    .sort((a, b) => a.dt - b.dt);
  let next = 0;
  for await (const line of createInterface({ input: createReadStream(tagsPath), crlfDelay: Infinity })) {
    if (!line) continue;
    const dt = dtOf(line);
    while (next < captured.length && captured[next]!.dt <= dt) yield { ...captured[next++]!, source: 'capture' };
    yield { line, dt, source: 'tags' };
  }
  while (next < captured.length) yield { ...captured[next++]!, source: 'capture' };
}

// The replayed input holds one wiki's edits for one hour, then only its tag changes, which can
// idle for minutes at night. A real outage check needs the all-wiki feed (DEFAULT_LABELLER).
export const REPLAY_LABELLER: LabellerOptions = { ...DEFAULT_LABELLER, outageGapMs: 30 * 60_000 };

/** Labels for every classifiable edit in the capture whose window the tags file covers. */
export async function labelReplay(
  capturePath: string,
  tagsPath: string,
  opts: LabellerOptions = REPLAY_LABELLER,
  onEvent?: (e: MergedEvent) => void,
): Promise<{ labels: Map<number, Label>; labeller: Labeller }> {
  const labeller = new Labeller(opts);
  const labels = new Map<number, Label>();
  let offset = 0;
  for await (const e of mergeByTime(capturePath, tagsPath)) {
    onEvent?.(e);
    for (const l of labeller.feed(e.line, String(offset++))) labels.set(l.revId, l);
  }
  return { labels, labeller };
}
