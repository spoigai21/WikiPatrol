// Run one predictor over one evaluation set, appending a row per edit (the result table, D12).
//
// Resumable: a restart skips edits already answered, so rate-limited free tiers can take days.
// On the sealed set it enforces the rules that make the score mean something: the set matches
// its checksum, the prompts match their tag, and a finished configuration is never rerun.

import { createHash } from 'node:crypto';
import { appendFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { gunzipSync } from 'node:zlib';
import { QuotaExhausted, RetryLater } from '../models/clients.ts';
import { manifestPath, setPath, type SetEdit } from './build-set.ts';
import type { Prediction, Predictor } from './predictors.ts';
import { promptsChangedSinceTag } from './prompts.ts';

export const SEALED = 'sealed';

export function loadSet(name: string): SetEdit[] {
  const jsonl = gunzipSync(readFileSync(setPath(name))).toString('utf8');
  const { sha256 } = JSON.parse(readFileSync(manifestPath(name), 'utf8')) as { sha256: string };
  const actual = createHash('sha256').update(jsonl).digest('hex');
  if (actual !== sha256) throw new Error(`set "${name}" does not match its manifest checksum (${actual} != ${sha256})`);
  return jsonl.trimEnd().split('\n').map((l) => JSON.parse(l) as SetEdit);
}

export const runPath = (set: string, config: string) => `results/phase5/runs/${set}/${config.replace(/[^\w.+-]+/g, '_')}.jsonl`;

export interface RunRow extends Prediction {
  config: string;
  set: string;
  at: string;
}

export function readRun(path: string): RunRow[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l) as RunRow);
}

export interface RunOptions {
  set: string;
  edits: readonly SetEdit[];
  predictor: Predictor;
  out?: string;
  signal?: AbortSignal;
  log?: (msg: string) => void;
  /** For tests: replaces the sealed-set checks. */
  checkSealed?: () => string[];
  sleep?: (ms: number) => Promise<void>;
  maxRetries?: number;
}

export interface RunResult {
  done: number;
  total: number;
  written: number;
  stopped?: string;
}

export async function runPredictor(opts: RunOptions): Promise<RunResult> {
  const { predictor, edits, set } = opts;
  const out = opts.out ?? runPath(set, predictor.config);
  const log = opts.log ?? (() => {});
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const maxRetries = opts.maxRetries ?? 8;

  mkdirSync(dirname(out), { recursive: true });
  // Two runners appending to one file would answer the same edits twice: one writer per file,
  // and what is already done is read only once the lock is held.
  const release = acquireLock(`${out}.lock`);
  try {
    const done = new Set(readRun(out).map((r) => r.revId));
    const todo = edits.filter((e) => !done.has(e.revId));
    if (set === SEALED) {
      if (todo.length === 0) throw new Error(`${predictor.config} is already scored on the sealed set; it is scored once (D12)`);
      const problems = opts.checkSealed ? opts.checkSealed() : predictor.usesPrompt ? promptsChangedSinceTag() : [];
      if (problems.length) throw new Error(`refusing the sealed set: prompts differ from their tag: ${problems.join(', ')}`);
    }
    log(`${predictor.config} on ${set}: ${done.size} done, ${todo.length} to go`);

    let written = 0;
    let last = 0;
    const result = (stopped?: string): RunResult => ({ done: done.size + written, total: edits.length, written, ...(stopped ? { stopped } : {}) });
    for (const e of todo) {
      if (opts.signal?.aborted) return result('aborted');
      let attempt = 0;
      for (;;) {
        const wait = last + predictor.minIntervalMs - Date.now();
        if (wait > 0) await sleep(wait);
        last = Date.now();
        try {
          const p = await predictor.predict(e);
          const row: RunRow = { ...p, config: predictor.config, set, at: new Date().toISOString() };
          appendFileSync(out, JSON.stringify(row) + '\n');
          written++;
          break;
        } catch (err) {
          if (err instanceof QuotaExhausted) {
            log(`stopping: ${err.message}`);
            return result('quota');
          }
          if (err instanceof RetryLater && attempt < maxRetries) {
            attempt++;
            const backoff = Math.min(err.retryAfterMs * 2 ** (attempt - 1), 5 * 60_000);
            log(`retry ${attempt}/${maxRetries} in ${Math.round(backoff / 1000)}s: ${err.message.slice(0, 120)}`);
            await sleep(backoff);
            continue;
          }
          // Nothing is written for a transport failure, so the edit is retried on the next run.
          log(`stopping on rev ${e.revId}: ${err instanceof Error ? err.message : String(err)}`);
          return result('error');
        }
      }
      if ((done.size + written) % 50 === 0) log(`${done.size + written}/${edits.length}`);
    }
    return result();
  } finally {
    release();
  }
}

function acquireLock(path: string): () => void {
  if (existsSync(path)) {
    const pid = Number(readFileSync(path, 'utf8'));
    let alive = false;
    try {
      process.kill(pid, 0);
      alive = true;
    } catch {
      /* stale lock from a process that died */
    }
    if (alive) throw new Error(`another runner (pid ${pid}) is writing ${path.replace(/\.lock$/, '')}`);
  }
  writeFileSync(path, String(process.pid));
  return () => rmSync(path, { force: true });
}
