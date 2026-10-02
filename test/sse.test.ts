import { describe, expect, it } from 'vitest';
import { SseParser, streamEvents } from '../src/stream/sse.ts';

describe('SseParser', () => {
  it('parses a complete event with id', () => {
    const p = new SseParser();
    expect(p.feed('event: message\nid: 7\ndata: {"a":1}\n\n')).toEqual([{ id: '7', event: 'message', data: '{"a":1}' }]);
    expect(p.lastEventId).toBe('7');
  });

  it('joins multi-line data and ignores comments', () => {
    const p = new SseParser();
    expect(p.feed(':ok\n\ndata: a\ndata: b\n\n')).toEqual([{ id: undefined, event: 'message', data: 'a\nb' }]);
  });

  it('handles an event split across arbitrary chunk boundaries', () => {
    const p = new SseParser();
    const text = 'id: 1\ndata: hello\n\nid: 2\ndata: world\n\n';
    const out = [...text].flatMap((ch) => p.feed(ch));
    expect(out.map((e) => [e.id, e.data])).toEqual([
      ['1', 'hello'],
      ['2', 'world'],
    ]);
  });

  it('does not double-dispatch when CRLF is split between chunks', () => {
    const p = new SseParser();
    const out = [...p.feed('data: x\r'), ...p.feed('\n\r'), ...p.feed('\n')];
    expect(out).toEqual([{ id: undefined, event: 'message', data: 'x' }]);
  });

  it('does not dispatch an event with no data', () => {
    expect(new SseParser().feed('id: 3\n\n')).toEqual([]);
  });
});

function sseResponse(body: string): Response {
  return new Response(new ReadableStream({
    start(c) {
      c.enqueue(new TextEncoder().encode(body));
      c.close();
    },
  }));
}

describe('streamEvents', () => {
  it('reconnects with Last-Event-ID after the server cuts the connection', async () => {
    const sentIds: (string | null)[] = [];
    const bodies = ['id: a\ndata: 1\n\nid: b\ndata: 2\n\n', 'id: c\ndata: 3\n\n'];
    const fetchImpl = (async (_url: string, init?: RequestInit) => {
      sentIds.push(new Headers(init?.headers).get('Last-Event-ID'));
      return sseResponse(bodies.shift() ?? '');
    }) as typeof fetch;

    const ctl = new AbortController();
    const got: string[] = [];
    for await (const ev of streamEvents('http://x', { signal: ctl.signal, fetchImpl, backoffMs: () => 0 })) {
      got.push(ev.data);
      if (got.length === 3) ctl.abort();
    }
    expect(got).toEqual(['1', '2', '3']);
    expect(sentIds.slice(0, 2)).toEqual([null, 'b']);
  });

  it('retries after a failed request', async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls++;
      if (calls === 1) throw new Error('boom');
      return sseResponse('id: z\ndata: ok\n\n');
    }) as unknown as typeof fetch;
    const ctl = new AbortController();
    for await (const ev of streamEvents('http://x', { signal: ctl.signal, fetchImpl, backoffMs: () => 0 })) {
      expect(ev.data).toBe('ok');
      ctl.abort();
    }
    expect(calls).toBe(2);
  });
});
