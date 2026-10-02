import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { parseArgs } from 'node:util';

export const STREAM_BASE = 'https://stream.wikimedia.org/v2/stream/';

export function args<T extends Record<string, { type: 'string' | 'boolean'; default?: string | boolean }>>(options: T) {
  return parseArgs({ options, strict: true }).values;
}

export function writeJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value, null, 2) + '\n');
}

/** Filesystem-safe UTC stamp, e.g. 2026-10-02T1933Z. */
export function stamp(d = new Date()): string {
  return d.toISOString().slice(0, 16).replace(':', '') + 'Z';
}

/** Aborts after `minutes`, or on Ctrl-C so a partial run still writes its report. */
export function runFor(minutes: number): AbortController {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), minutes * 60_000);
  process.once('SIGINT', () => {
    clearTimeout(timer);
    ctl.abort();
  });
  return ctl;
}

export function log(msg: string): void {
  process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
}
