// Replay a past time window from EventStreams' history (7-31 days) via `?since=`.
//
// A replay merges one Kafka topic per datacenter, each replayed in turn: the near-idle standby
// topic can run all the way to the present before the busy one starts. So the window ends only
// once every topic has moved past it, plus a little slack for disorder within a topic. The full
// topic list comes from the SSE id, which carries a position for each topic.

import { streamEvents, type SseEvent, type StreamOptions } from './sse.ts';

export const STOP_GRACE_MS = 120_000;

export interface ReplayedEvent {
  ev: SseEvent;
  /** meta.dt in epoch ms. */
  dt: number;
  /** meta.topic, or meta.stream when absent. */
  topic: string;
}

export function topicsInId(id: string | undefined): string[] {
  try {
    const pos = JSON.parse(id ?? '') as { topic?: unknown }[];
    return pos.map((p) => p.topic).filter((t): t is string => typeof t === 'string');
  } catch {
    return [];
  }
}

/**
 * Yields events whose meta.dt lies in [since, until), in stream order, and returns once every
 * topic has passed `until + STOP_GRACE_MS`. Unparseable events are skipped and counted.
 */
export async function* replayWindow(
  baseUrl: string,
  since: Date,
  until: Date,
  opts: StreamOptions & { onUnparseable?: () => void } = {},
): AsyncGenerator<ReplayedEvent> {
  const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}since=${encodeURIComponent(since.toISOString())}`;
  const start = since.getTime();
  const end = until.getTime();
  const topicsAll = new Set<string>();
  const topicsPast = new Set<string>();
  for await (const ev of streamEvents(url, opts)) {
    let meta: { dt?: unknown; topic?: unknown; stream?: unknown };
    try {
      meta = (JSON.parse(ev.data) as { meta?: typeof meta }).meta ?? {};
    } catch {
      opts.onUnparseable?.();
      continue;
    }
    const dt = typeof meta.dt === 'string' ? Date.parse(meta.dt) : NaN;
    const topic = typeof meta.topic === 'string' ? meta.topic : String(meta.stream ?? '');
    if (Number.isNaN(dt) || !topic) {
      opts.onUnparseable?.();
      continue;
    }
    topicsAll.add(topic);
    for (const t of topicsInId(ev.id)) topicsAll.add(t);
    if (dt >= end + STOP_GRACE_MS) {
      topicsPast.add(topic);
      if (topicsPast.size === topicsAll.size) return;
    }
    if (dt < start || dt >= end) continue;
    yield { ev, dt, topic };
  }
}
