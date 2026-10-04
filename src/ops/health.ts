// Liveness and readiness over HTTP, for Kubernetes probes (Phase 8) and `docker compose`.
//
// Probes that only say "the process is up" are useless; these fail when the service is sick:
//   /livez   — every registered check is still making progress (a stuck consumer, a dead stream)
//   /readyz  — the service has connected and started doing its job
// A failing liveness probe gets the container restarted; it resumes from its checkpoint.

import { createServer, type Server } from 'node:http';

export interface Check {
  name: string;
  /** Healthy, or a reason it is not. */
  live: () => Promise<true | string> | true | string;
  ready: () => Promise<true | string> | true | string;
}

export class Health {
  private readonly checks: Check[] = [];

  add(check: Check): void {
    this.checks.push(check);
  }

  async evaluate(kind: 'live' | 'ready'): Promise<{ ok: boolean; checks: Record<string, true | string> }> {
    const results: Record<string, true | string> = {};
    for (const c of this.checks) {
      try {
        results[c.name] = await c[kind]();
      } catch (err) {
        results[c.name] = `check threw: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
    return { ok: Object.values(results).every((r) => r === true), checks: results };
  }

  /** Serve /livez and /readyz on `port` (PORT env, default 8080). Returns the server. */
  serve(port = Number(process.env.PORT ?? 8080)): Server {
    const server = createServer(async (req, res) => {
      const kind = req.url === '/livez' ? 'live' : req.url === '/readyz' ? 'ready' : undefined;
      if (!kind) {
        res.writeHead(404).end();
        return;
      }
      const r = await this.evaluate(kind);
      res.writeHead(r.ok ? 200 : 503, { 'Content-Type': 'application/json' }).end(JSON.stringify(r));
    });
    server.listen(port);
    return server;
  }
}

/**
 * Progress tracker for a long-running loop: live while it has made progress within `stallMs`, or
 * while there is nothing to do (`idle()` says so). Ready once it has started.
 */
export class Progress {
  private last = Date.now();
  private started = false;

  constructor(
    private readonly stallMs: number,
    private readonly idle: () => Promise<boolean> | boolean = () => false,
    private readonly now: () => number = Date.now,
  ) {}

  tick(): void {
    this.last = this.now();
    this.started = true;
  }

  start(): void {
    this.started = true;
    this.last = this.now();
  }

  async live(): Promise<true | string> {
    const since = this.now() - this.last;
    if (since <= this.stallMs) return true;
    if (await this.idle()) return true;
    return `no progress for ${Math.round(since / 1000)}s with work waiting`;
  }

  ready(): true | string {
    return this.started ? true : 'not started';
  }
}
