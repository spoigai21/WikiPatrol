// Minimal Server-Sent Events client for Wikimedia EventStreams.
// The parser is pure so it can be tested without a network; the reader adds
// reconnect-with-Last-Event-ID, which the 15-minute server-side cut makes mandatory.

export const USER_AGENT = 'WikiPatrol/0.0 (https://github.com/spoigai21/WikiPatrol)';

export interface SseEvent {
  id: string | undefined;
  event: string;
  data: string;
}

export class SseParser {
  private buffer = '';
  private data: string[] = [];
  private eventType = '';
  private id: string | undefined;
  /** Last id seen on the stream, which is what a reconnect must send. */
  lastEventId: string | undefined;

  feed(chunk: string): SseEvent[] {
    this.buffer += chunk;
    const out: SseEvent[] = [];
    // A trailing '\r' may be the first half of '\r\n', so leave it for the next chunk.
    const lines = this.buffer.split(/\r\n|\n|\r(?!$)/);
    this.buffer = lines.pop() ?? '';
    for (const line of lines) {
      if (line === '') {
        if (this.data.length > 0) {
          out.push({ id: this.id, event: this.eventType || 'message', data: this.data.join('\n') });
        }
        this.data = [];
        this.eventType = '';
        continue;
      }
      if (line.startsWith(':')) continue;
      const colon = line.indexOf(':');
      const field = colon === -1 ? line : line.slice(0, colon);
      let value = colon === -1 ? '' : line.slice(colon + 1);
      if (value.startsWith(' ')) value = value.slice(1);
      switch (field) {
        case 'data':
          this.data.push(value);
          break;
        case 'event':
          this.eventType = value;
          break;
        case 'id':
          if (!value.includes('\0')) {
            this.id = value;
            this.lastEventId = value;
          }
          break;
        // 'retry' and unknown fields are ignored.
      }
    }
    return out;
  }
}

export interface StreamOptions {
  signal?: AbortSignal;
  lastEventId?: string;
  onConnect?: (attempt: number, lastEventId: string | undefined) => void;
  onDisconnect?: (reason: string) => void;
  /** Injected for tests. */
  fetchImpl?: typeof fetch;
  backoffMs?: (attempt: number) => number;
}

const defaultBackoff = (attempt: number) => Math.min(30_000, 500 * 2 ** Math.min(attempt, 6));

/**
 * Yields events forever (until aborted), reconnecting with Last-Event-ID so a
 * dropped connection picks up where it left off instead of at "now".
 */
export async function* streamEvents(url: string, opts: StreamOptions = {}): AsyncGenerator<SseEvent> {
  const doFetch = opts.fetchImpl ?? fetch;
  const backoff = opts.backoffMs ?? defaultBackoff;
  let lastEventId = opts.lastEventId;
  let failures = 0;

  while (!opts.signal?.aborted) {
    const parser = new SseParser();
    opts.onConnect?.(failures, lastEventId);
    try {
      const headers: Record<string, string> = { Accept: 'text/event-stream', 'User-Agent': USER_AGENT };
      if (lastEventId !== undefined) headers['Last-Event-ID'] = lastEventId;
      const init: RequestInit = { headers };
      if (opts.signal) init.signal = opts.signal;
      const res = await doFetch(url, init);
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      const decoder = new TextDecoder();
      for await (const chunk of res.body as unknown as AsyncIterable<Uint8Array>) {
        for (const ev of parser.feed(decoder.decode(chunk, { stream: true }))) {
          failures = 0;
          if (ev.id !== undefined) lastEventId = ev.id;
          yield ev;
        }
      }
      opts.onDisconnect?.('stream ended');
    } catch (err) {
      if (opts.signal?.aborted) return;
      failures++;
      opts.onDisconnect?.(err instanceof Error ? err.message : String(err));
    }
    if (opts.signal?.aborted) return;
    await sleep(backoff(failures), opts.signal);
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => {
      clearTimeout(t);
      resolve();
    }, { once: true });
  });
}
