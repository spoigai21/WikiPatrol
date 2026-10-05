// The classifier: wiki.scored -> one model tier -> wiki.predictions (D14).
//
// This is the consumer Phase 8 scales on lag. wiki.scored has SCORED_PARTITIONS partitions, so up
// to that many replicas share it as one consumer group. Delivery is at-least-once: an edit can be
// classified twice after a rebalance, and wiki.predictions is keyed by rev id so readers keep one.
// Edits the filter dropped are passed through as "not flagged" without calling anything.

import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { chunkBySize, ensureKeptTopic } from '../kafka/topics.ts';
import { fetchDiff } from '../labels/action-api.ts';
import { clientFor } from '../models/clients.ts';
import type { SetEdit } from '../phase5/build-set.ts';
import { renderDiff } from '../phase5/diff.ts';
import { filterTemp, LlmPredictor, type Predictor } from '../phase5/predictors.ts';
import { PROMPT_IDS, type PromptId } from '../phase5/prompts.ts';
import { accountAgeDays } from '../filter/rules.ts';
import type { ScoredRecord } from '../pipeline/stages.ts';

export interface PredictionRecord {
  revId: number;
  editTime: string;
  config: string;
  tier: 'filter' | 'model';
  revert: boolean | null;
  pRevert: number | null;
  invalid?: string;
  latencyMs: number;
  tokensIn: number | null;
  tokensOut: number | null;
  modelVersion: string;
  classifiedAt: string;
  source: { partition: number; offset: string };
}

/**
 * "heuristic"; "slow-heuristic:<ms>" — the heuristic at a fixed pace, standing in for a
 * rate-limited model tier in scaling demos and CI; or "ollama:<model>:<prompt>" (also groq:,
 * gemini:).
 */
export function predictorFor(config: string): Predictor {
  if (config === 'heuristic') return filterTemp;
  const slow = /^slow-heuristic:(\d+)$/.exec(config);
  if (slow) return { ...filterTemp, config: `baseline:filter+temporary@${slow[1]}ms`, minIntervalMs: Number(slow[1]) };
  const parts = config.split(':');
  const prompt = parts.pop() as PromptId;
  if (!(PROMPT_IDS as readonly string[]).includes(prompt)) throw new Error(`classifier config "${config}" must end in a prompt id (${PROMPT_IDS.join(', ')})`);
  return new LlmPredictor(clientFor(parts.join(':')), prompt);
}

/** What a model needs about an edit, built from the pipeline record plus a fresh diff. */
export async function toSetEdit(r: ScoredRecord, needDiff: boolean): Promise<SetEdit> {
  let diff = '';
  let diffTruncated = false;
  let diffUnavailable: string | undefined;
  if (needDiff) {
    const d = await fetchDiff(r.revId, r.isNew);
    if (d.kind === 'diff') ({ text: diff, truncated: diffTruncated } = renderDiff(d.html));
    else if (d.kind === 'created') {
      diffTruncated = d.text.length > 3000;
      diff = diffTruncated ? d.text.slice(0, 3000) + '\n[page text truncated]' : d.text;
    } else diffUnavailable = d.reason;
  }
  return {
    revId: r.revId,
    title: r.title,
    editTime: new Date(r.timestamp * 1000).toISOString(),
    isNew: r.isNew,
    minor: r.minor,
    userClass: r.userClass,
    accountAgeDays: r.account ? accountAgeDays(r.account, r.timestamp) : null,
    editcount: r.account?.editcount ?? null,
    sizeDelta: (r.lengthNew ?? 0) - (r.lengthOld ?? 0),
    comment: r.comment,
    diff,
    diffTruncated,
    ...(diffUnavailable ? { diffUnavailable } : {}),
    label: 'not-reverted', // unknown at classification time; never shown to a model
  };
}

export interface ClassifierOptions {
  brokers: string[];
  input: string;
  output: string;
  config: string;
  groupId?: string;
  signal?: AbortSignal;
  log?: (msg: string) => void;
  onProcessed?: (partition: number, offset: string) => void;
  /** Tests inject a predictor instead of a config. */
  predictor?: Predictor;
  /**
   * Minimum time between model calls in this replica, on top of the model's own limit: stands in
   * for a per-replica quota (one API key each) in the Phase 8 run (D16).
   */
  paceMs?: number;
}

export async function runClassifier(opts: ClassifierOptions): Promise<void> {
  const log = opts.log ?? (() => {});
  const base = opts.predictor ?? predictorFor(opts.config);
  const predictor: Predictor = opts.paceMs && opts.paceMs > base.minIntervalMs ? { ...base, predict: (e) => base.predict(e), minIntervalMs: opts.paceMs } : base;
  const kafka = new Kafka({ clientId: `wikipatrol-classifier-${randomUUID().slice(0, 8)}`, brokers: opts.brokers, logLevel: logLevel.WARN });
  const admin = kafka.admin();
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
  // Short timeouts: the classifier heartbeats after every model call (seconds apart), and its
  // replicas come and go with the autoscaler. A long session would keep dead replicas in the group
  // for minutes, stalling every rebalance until liveness probes restart the live ones too (D16).
  const consumer = kafka.consumer({ groupId: opts.groupId ?? 'wikipatrol-classifier', sessionTimeout: 45_000, rebalanceTimeout: 60_000, heartbeatInterval: 5_000 });

  await admin.connect();
  await ensureKeptTopic(admin, opts.output);
  await admin.disconnect();
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: opts.input, fromBeginning: true });
  log(`classifier (${predictor.config}${predictor.minIntervalMs ? `, at most one model call per ${predictor.minIntervalMs} ms` : ''}): ${opts.input} -> ${opts.output}`);

  let last = 0;
  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      const out: { key: string; value: string }[] = [];
      let lastOffset: string | undefined;
      for (const m of batch.messages) {
        if (!isRunning() || isStale() || opts.signal?.aborted) break;
        const r = JSON.parse(m.value!.toString()) as ScoredRecord;
        const base = { revId: r.revId, editTime: new Date(r.timestamp * 1000).toISOString(), classifiedAt: '', source: { partition: batch.partition, offset: m.offset } };
        let rec: PredictionRecord;
        if (!r.filter.keep) {
          rec = { ...base, config: `filter:${r.filter.policy}`, tier: 'filter', revert: false, pRevert: 0, latencyMs: 0, tokensIn: null, tokensOut: null, modelVersion: r.filter.rule };
        } else {
          const wait = last + predictor.minIntervalMs - Date.now();
          if (wait > 0) await new Promise((res) => setTimeout(res, wait));
          last = Date.now();
          const p = await predictor.predict(await toSetEdit(r, predictor.usesPrompt));
          rec = { ...base, config: predictor.config, tier: 'model', revert: p.revert, pRevert: p.pRevert, ...(p.invalid ? { invalid: p.invalid } : {}), latencyMs: p.latencyMs, tokensIn: p.tokensIn, tokensOut: p.tokensOut, modelVersion: p.modelVersion };
        }
        rec.classifiedAt = new Date().toISOString();
        out.push({ key: String(r.revId), value: JSON.stringify(rec) });
        lastOffset = m.offset;
        // Model calls are slow: write and commit as we go, so a rebalance loses little work.
        if (rec.tier === 'model' || out.length >= 500) {
          for (const chunk of chunkBySize(out.splice(0))) await producer.send({ topic: opts.output, acks: -1, messages: chunk });
          resolveOffset(m.offset);
          await consumer.commitOffsets([{ topic: opts.input, partition: batch.partition, offset: String(BigInt(m.offset) + 1n) }]);
          opts.onProcessed?.(batch.partition, m.offset);
          await heartbeat();
        }
      }
      if (out.length && lastOffset !== undefined) {
        for (const chunk of chunkBySize(out.splice(0))) await producer.send({ topic: opts.output, acks: -1, messages: chunk });
        resolveOffset(lastOffset);
        await consumer.commitOffsets([{ topic: opts.input, partition: batch.partition, offset: String(BigInt(lastOffset) + 1n) }]);
        opts.onProcessed?.(batch.partition, lastOffset);
      }
    },
  });

  await new Promise<void>((resolve) => {
    if (opts.signal?.aborted) resolve();
    opts.signal?.addEventListener('abort', () => resolve(), { once: true });
  });
  await consumer.disconnect();
  await producer.disconnect();
}
