import { z } from 'zod';

// The ingester stores events raw. It parses only far enough to validate the
// envelope and read the id it dedupes on; the payload is never interpreted here.
export const Envelope = z.looseObject({
  meta: z.looseObject({
    id: z.string().min(1),
    stream: z.string().min(1),
    dt: z.string().min(1),
  }),
});

export interface RawRecord {
  /** meta.id: unique per event, stable across replays. The dedupe key. */
  id: string;
  /** The event's data line, byte-for-byte as received. */
  data: string;
  /** The SSE id of this event: where to reconnect from if it is the last one stored. */
  eventId?: string;
}

/** Remembers the most recent `capacity` ids. Replays after a reconnect overlap by seconds, not days. */
export class RecentIds {
  private readonly order: string[] = [];
  private readonly set = new Set<string>();
  private head = 0;

  constructor(private readonly capacity = 100_000) {}

  /** Returns false if the id was already seen. */
  add(id: string): boolean {
    if (this.set.has(id)) return false;
    if (this.order.length < this.capacity) this.order.push(id);
    else {
      this.set.delete(this.order[this.head]!);
      this.order[this.head] = id;
      this.head = (this.head + 1) % this.capacity;
    }
    this.set.add(id);
    return true;
  }
}
