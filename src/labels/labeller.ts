// Phase 4: revert labels, for free, joined from the raw log (DECISIONS.md D2, D4, D11).
//
// Pure and deterministic: feed it the raw topic in offset order and it emits one label per
// classifiable edit once that edit's window has closed *in event time*. The same offsets always
// give the same labels, whether read live, replayed, or resumed after a restart.

import { isClassifiable, RecentChange, TagsChange, addedTags } from '../phase0/events.ts';

export type LabelValue = 'reverted' | 'not-reverted' | 'deleted' | 'incomplete';

export interface Label {
  revId: number;
  wiki: string;
  title: string;
  /** The edit's own time (recentchange `timestamp`), ISO. */
  editTime: string;
  label: LabelValue;
  /** When `mw-reverted` was added, if inside the window. */
  revertedAt?: string;
  revertDelaySeconds?: number;
  windowHours: number;
  /** Offset of the edit's event in the raw topic: the labeller's checkpoint. */
  sourceOffset: string;
}

export interface LabellerOptions {
  wiki: string;
  /** The label window (D4: 72h). */
  windowMs: number;
  /** Extra event time to wait past the window, for events that arrive slightly out of order. */
  graceMs: number;
  /**
   * A jump in event time larger than this means the log has a hole (ingester down past the
   * stream's replay window). Edits whose window overlaps a hole are labelled `incomplete`.
   */
  outageGapMs: number;
}

export const DEFAULT_LABELLER: LabellerOptions = {
  wiki: 'enwiki',
  windowMs: 72 * 3_600_000,
  graceMs: 10 * 60_000,
  // The all-wiki feed never idles for two minutes (D8: 33/s at its quietest).
  outageGapMs: 2 * 60_000,
};

interface Pending {
  revId: number;
  title: string;
  editMs: number;
  offset: string;
  revertedMs?: number;
  deleted: boolean;
  incomplete: boolean;
}

export interface LabellerStats {
  events: number;
  unparseable: number;
  outages: { from: string; to: string }[];
}

export class Labeller {
  private readonly queue: Pending[] = [];
  private head = 0;
  private readonly byRev = new Map<number, Pending>();
  private readonly byTitle = new Map<string, Set<Pending>>();
  private watermark = -Infinity;
  readonly stats: LabellerStats = { events: 0, unparseable: 0, outages: [] };

  constructor(private readonly opts: LabellerOptions = DEFAULT_LABELLER) {}

  /** One raw event, in log order. Returns the labels whose window this event closed. */
  feed(data: string, offset: string): Label[] {
    this.stats.events++;
    let json: unknown;
    try {
      json = JSON.parse(data);
    } catch {
      this.stats.unparseable++;
      return [];
    }
    const stream = (json as { meta?: { stream?: unknown } }).meta?.stream;
    let dt: number;
    if (stream === 'mediawiki.revision-tags-change') {
      const p = TagsChange.safeParse(json);
      if (!p.success) return this.unparseable();
      dt = Date.parse(p.data.meta.dt);
      this.advance(dt);
      if (p.data.database === this.opts.wiki && addedTags(p.data).includes('mw-reverted')) {
        const e = this.byRev.get(p.data.rev_id);
        if (e && e.revertedMs === undefined) e.revertedMs = dt;
      }
    } else {
      const p = RecentChange.safeParse(json);
      if (!p.success) return this.unparseable();
      const rc = p.data;
      dt = Date.parse(rc.meta.dt);
      this.advance(dt);
      if (rc.wiki === this.opts.wiki) {
        if (isClassifiable(rc) && rc.revision && rc.timestamp !== undefined && !this.byRev.has(rc.revision.new)) {
          const e: Pending = { revId: rc.revision.new, title: rc.title ?? '', editMs: rc.timestamp * 1000, offset, deleted: false, incomplete: false };
          this.queue.push(e);
          this.byRev.set(e.revId, e);
          let set = this.byTitle.get(e.title);
          if (!set) this.byTitle.set(e.title, (set = new Set()));
          set.add(e);
        } else if (rc.type === 'log' && rc.log_type === 'delete' && rc.log_action === 'delete' && rc.title) {
          for (const e of this.byTitle.get(rc.title) ?? []) if (dt <= e.editMs + this.opts.windowMs) e.deleted = true;
        }
      }
    }
    return this.emitReady();
  }

  /** Edits still inside their window. */
  get pending(): number {
    return this.queue.length - this.head;
  }

  private unparseable(): Label[] {
    this.stats.unparseable++;
    return [];
  }

  private advance(dt: number): void {
    if (Number.isNaN(dt)) return;
    if (dt - this.watermark > this.opts.outageGapMs && this.watermark !== -Infinity) {
      this.stats.outages.push({ from: new Date(this.watermark).toISOString(), to: new Date(dt).toISOString() });
      // Every edit still waiting has a window reaching past the old watermark, so the hole is in it.
      for (let i = this.head; i < this.queue.length; i++) {
        const e = this.queue[i]!;
        if (e.editMs <= dt) e.incomplete = true;
      }
    }
    if (dt > this.watermark) this.watermark = dt;
  }

  private emitReady(): Label[] {
    const out: Label[] = [];
    const { windowMs, graceMs } = this.opts;
    while (this.head < this.queue.length && this.watermark >= this.queue[this.head]!.editMs + windowMs + graceMs) {
      const e = this.queue[this.head++]!;
      this.byRev.delete(e.revId);
      const set = this.byTitle.get(e.title);
      set?.delete(e);
      if (set?.size === 0) this.byTitle.delete(e.title);
      out.push(this.toLabel(e));
    }
    // Drop emitted entries now and then so the queue does not grow without bound.
    if (this.head > 10_000 && this.head * 2 > this.queue.length) {
      this.queue.splice(0, this.head);
      this.head = 0;
    }
    return out;
  }

  private toLabel(e: Pending): Label {
    const base = {
      revId: e.revId,
      wiki: this.opts.wiki,
      title: e.title,
      editTime: new Date(e.editMs).toISOString(),
      windowHours: this.opts.windowMs / 3_600_000,
      sourceOffset: e.offset,
    };
    if (e.revertedMs !== undefined && e.revertedMs <= e.editMs + this.opts.windowMs) {
      return {
        ...base,
        label: 'reverted',
        revertedAt: new Date(e.revertedMs).toISOString(),
        revertDelaySeconds: Math.round((e.revertedMs - e.editMs) / 1000),
      };
    }
    if (e.deleted) return { ...base, label: 'deleted' };
    if (e.incomplete) return { ...base, label: 'incomplete' };
    return { ...base, label: 'not-reverted' };
  }
}
