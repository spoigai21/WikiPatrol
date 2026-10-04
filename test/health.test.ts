import { afterEach, describe, expect, it } from 'vitest';
import { Health, Progress } from '../src/ops/health.ts';

describe('health probes', () => {
  let close: (() => void) | undefined;
  afterEach(() => close?.());

  it('fail liveness when a loop stalls with work waiting, and not when it is merely idle', async () => {
    let t = 0;
    let waiting = true;
    const p = new Progress(1000, () => !waiting, () => t);
    expect(p.ready()).toBe('not started');
    p.start();
    expect(await p.live()).toBe(true);
    t = 5000;
    expect(await p.live()).toMatch(/no progress for 5s/);
    waiting = false;
    expect(await p.live()).toBe(true);
    p.tick();
    waiting = true;
    expect(await p.live()).toBe(true);
  });

  it('serves 200 when every check passes and 503 naming the one that does not', async () => {
    const h = new Health();
    let sick = false;
    h.add({ name: 'a', live: () => true, ready: () => true });
    h.add({ name: 'b', live: () => (sick ? 'stuck' : true), ready: () => true });
    const server = h.serve(0);
    close = () => server.close();
    await new Promise((r) => server.once('listening', r));
    const port = (server.address() as { port: number }).port;
    expect((await fetch(`http://127.0.0.1:${port}/livez`)).status).toBe(200);
    sick = true;
    const res = await fetch(`http://127.0.0.1:${port}/livez`);
    expect(res.status).toBe(503);
    expect(await res.json()).toEqual({ ok: false, checks: { a: true, b: 'stuck' } });
    expect((await fetch(`http://127.0.0.1:${port}/readyz`)).status).toBe(200);
  });
});
