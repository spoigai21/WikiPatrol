// A local HTTP server that plays EventStreams for tests: it honours Last-Event-ID,
// replays a few events of overlap on reconnect (at-least-once, like the real thing),
// and kills the socket at random points, including halfway through writing an event.

import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { mulberry32 } from '../src/phase0/stats.ts';

export const N = 500;
const OVERLAP = 3;
export const payload = (i: number) =>
  JSON.stringify({ meta: { id: `e${i}`, stream: 'mediawiki.recentchange', dt: new Date(1_790_000_000_000 + i).toISOString() }, n: i });
const frame = (i: number) => `id: ${i}\ndata: ${payload(i)}\n\n`;

export interface FakeStream {
  url: string;
  server: Server;
  connections: number;
  reconnectIds: (string | undefined)[];
}

/** `paceMs` spaces events out, for tests that must stop a consumer partway through the stream. */
export function startFakeStream(seed: number, extra: (i: number) => string | undefined = () => undefined, paceMs = 0): Promise<FakeStream> {
  const rand = mulberry32(seed);
  const state: FakeStream = { url: '', server: undefined as unknown as Server, connections: 0, reconnectIds: [] };
  state.server = createServer((req, res) => {
    state.connections++;
    const last = req.headers['last-event-id'] as string | undefined;
    state.reconnectIds.push(last);
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.write(':ok\n\n');
    let i = last ? Math.max(1, Number(last) + 1 - OVERLAP) : 1;
    // Cut after a random number of events, at a random byte offset into the next one.
    const cutAfter = 20 + Math.floor(rand() * 80);
    let sent = 0;
    const tick = () => {
      if (i > N) return; // caught up: stay connected and idle, like a quiet stream
      if (sent === cutAfter) {
        const f = frame(i);
        res.write(f.slice(0, Math.floor(rand() * f.length)));
        res.socket?.destroy();
        return;
      }
      const injected = extra(i);
      if (injected) res.write(injected);
      res.write(frame(i));
      i++;
      sent++;
      if (paceMs > 0) setTimeout(tick, paceMs);
      else setImmediate(tick);
    };
    tick();
  });
  return new Promise((resolve) =>
    state.server.listen(0, '127.0.0.1', () => {
      state.url = `http://127.0.0.1:${(state.server.address() as AddressInfo).port}/`;
      resolve(state);
    }),
  );
}

export async function untilCount(get: () => number, n: number, ctl: AbortController, timeoutMs = 10_000) {
  const start = Date.now();
  while (get() < n && Date.now() - start < timeoutMs) await new Promise((r) => setTimeout(r, 10));
  ctl.abort();
}

export const expectedIds = Array.from({ length: N }, (_, i) => `e${i + 1}`);

