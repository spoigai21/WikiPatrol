// Phase 1 done-criterion: a forced disconnect loses nothing and duplicates nothing.
// See fake-stream.ts for how the disconnects are produced.

import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { RecentIds } from '../src/ingest/envelope.ts';
import { runIngester } from '../src/ingest/ingester.ts';
import { FileSink, MemorySink } from '../src/ingest/sink.ts';
import { expectedIds, N, payload, startFakeStream, untilCount, type FakeStream } from './fake-stream.ts';

describe('ingester under forced disconnects', () => {
  let fake: FakeStream;
  afterEach(() => fake?.server.close());

  it.each([1, 2, 3, 4, 5])('loses nothing and duplicates nothing (seed %i)', async (seed) => {
    fake = await startFakeStream(seed);
    const sink = new MemorySink();
    const ctl = new AbortController();
    const run = runIngester({ url: fake.url, sink, signal: ctl.signal, batchSize: 7, flushMs: 20, backoffMs: () => 0 });
    await untilCount(() => sink.records.length, N, ctl);
    const stats = await run;

    expect(sink.records.map((r) => r.id)).toEqual(expectedIds);
    expect(sink.records.map((r) => r.data)).toEqual(expectedIds.map((_, i) => payload(i + 1)));
    expect(fake.connections).toBeGreaterThan(3);
    expect(stats.duplicates).toBeGreaterThan(0); // the overlap was really replayed, and dropped
    expect(fake.reconnectIds.slice(1).every((id) => id !== undefined)).toBe(true);
  });

  it('routes invalid payloads away from the sink', async () => {
    fake = await startFakeStream(9, (i) => (i === 10 ? 'data: {"not":"an event"}\n\n' : undefined));
    const sink = new MemorySink();
    const invalid: string[] = [];
    const ctl = new AbortController();
    const run = runIngester({
      url: fake.url,
      sink,
      signal: ctl.signal,
      flushMs: 20,
      backoffMs: () => 0,
      onInvalid: (d) => invalid.push(d),
    });
    await untilCount(() => sink.records.length, N, ctl);
    await run;
    expect(sink.records.map((r) => r.id)).toEqual(expectedIds);
    expect(invalid.length).toBeGreaterThan(0);
    expect(invalid.every((d) => d === '{"not":"an event"}')).toBe(true);
  });
});

describe('ingester across a process restart', () => {
  let dir: string;
  let fake: FakeStream;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'wikipatrol-'));
  });
  afterEach(() => {
    fake?.server.close();
    rmSync(dir, { recursive: true, force: true });
  });

  it('resumes from the file checkpoint and the output is byte-identical to the source', async () => {
    fake = await startFakeStream(7);
    const lines = () => {
      try {
        return readFileSync(join(dir, 'raw.jsonl'), 'utf8').split('\n').filter(Boolean).length;
      } catch {
        return 0;
      }
    };

    // First process: stopped partway through.
    const ctl1 = new AbortController();
    const run1 = runIngester({ url: fake.url, sink: new FileSink(dir), signal: ctl1.signal, batchSize: 5, flushMs: 20, backoffMs: () => 0 });
    await untilCount(lines, 180, ctl1);
    await run1;
    expect(lines()).toBeLessThan(N);

    // Second process: fresh memory, same directory.
    const ctl2 = new AbortController();
    const run2 = runIngester({ url: fake.url, sink: new FileSink(dir), signal: ctl2.signal, batchSize: 5, flushMs: 20, backoffMs: () => 0 });
    await untilCount(lines, N, ctl2);
    await run2;

    const want = expectedIds.map((_, i) => payload(i + 1) + '\n').join('');
    expect(readFileSync(join(dir, 'raw.jsonl'), 'utf8')).toBe(want);
  });
});

describe('RecentIds', () => {
  it('forgets the oldest id once full', () => {
    const r = new RecentIds(2);
    expect(r.add('a')).toBe(true);
    expect(r.add('b')).toBe(true);
    expect(r.add('a')).toBe(false);
    expect(r.add('c')).toBe(true);
    expect(r.add('a')).toBe(true);
  });
});
