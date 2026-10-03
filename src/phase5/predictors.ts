// Everything that fills a row of the grid: an LLM with a prompt, or a baseline (D12).

import { USER_AGENT } from '../stream/sse.ts';
import { RetryLater, type ModelClient } from '../models/clients.ts';
import type { SetEdit } from './build-set.ts';
import { buildMessages, parseAnswer, type PromptId } from './prompts.ts';

export interface Prediction {
  revId: number;
  /** null = the predictor answered but not in the contract; scored as "not flagged" (D12). */
  revert: boolean | null;
  pRevert: number | null;
  invalid?: string;
  /** The raw answer, capped, for auditing. */
  raw?: string;
  latencyMs: number;
  tokensIn: number | null;
  tokensOut: number | null;
  modelVersion: string;
}

export interface Predictor {
  /** Config name: the row label in the grid and the run file name. */
  readonly config: string;
  readonly minIntervalMs: number;
  readonly usesPrompt: boolean;
  predict(e: SetEdit): Promise<Prediction>;
}

export class LlmPredictor implements Predictor {
  readonly config: string;
  readonly usesPrompt = true;
  constructor(private readonly client: ModelClient, private readonly prompt: PromptId) {
    this.config = `${client.id}__${prompt}`;
  }
  get minIntervalMs() {
    return this.client.minIntervalMs;
  }
  async predict(e: SetEdit): Promise<Prediction> {
    const c = await this.client.complete(buildMessages(this.prompt, e));
    const parsed = parseAnswer(c.text);
    const base = { revId: e.revId, latencyMs: Math.round(c.latencyMs), tokensIn: c.tokensIn, tokensOut: c.tokensOut, modelVersion: c.modelVersion, raw: c.text.slice(0, 2000) };
    return parsed.ok
      ? { ...base, revert: parsed.answer.revert, pRevert: parsed.answer.p_revert }
      : { ...base, revert: null, pRevert: null, invalid: parsed.error };
  }
}

const instant = (e: SetEdit, revert: boolean, version: string): Prediction => ({
  revId: e.revId, revert, pRevert: revert ? 1 : 0, latencyMs: 0, tokensIn: null, tokensOut: null, modelVersion: version,
});

/** The Phase 3 filter alone: everything it lets through is flagged. */
export const filterOnly: Predictor = {
  config: 'baseline:filter-only', minIntervalMs: 0, usesPrompt: false,
  predict: async (e) => instant(e, true, 'filter:extendedconfirmed'),
};

/** Filter, then flag only temporary accounts. */
export const filterTemp: Predictor = {
  config: 'baseline:filter+temporary', minIntervalMs: 0, usesPrompt: false,
  predict: async (e) => instant(e, e.userClass === 'temporary', 'filter:extendedconfirmed+temporary'),
};

/** A Wikimedia LiftWing model, flagged at p >= 0.5. */
export class LiftWingPredictor implements Predictor {
  readonly config: string;
  readonly usesPrompt = false;
  readonly minIntervalMs = 100;
  constructor(private readonly model: 'revertrisk-language-agnostic' | 'enwiki-damaging') {
    this.config = `baseline:liftwing-${model}`;
  }
  async predict(e: SetEdit): Promise<Prediction> {
    const t0 = performance.now();
    const res = await fetch(`https://api.wikimedia.org/service/lw/inference/v1/models/${this.model}:predict`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'User-Agent': USER_AGENT },
      body: JSON.stringify({ rev_id: e.revId, lang: 'en' }),
    });
    const text = await res.text();
    if (res.status === 429 || res.status >= 500) throw new RetryLater(`liftwing ${res.status}`, 10_000);
    const latencyMs = Math.round(performance.now() - t0);
    let p: number | undefined;
    let version: string = this.model;
    try {
      const j = JSON.parse(text) as Record<string, any>;
      if (this.model === 'revertrisk-language-agnostic') {
        p = j.output?.probabilities?.true;
        version = `${this.model}@${j.model_version ?? '?'}`;
      } else {
        const d = j.enwiki?.scores?.[String(e.revId)]?.damaging;
        p = d?.score?.probability?.true;
        version = `${this.model}@${j.enwiki?.models?.damaging?.version ?? '?'}`;
      }
    } catch {
      /* fall through to invalid */
    }
    if (typeof p !== 'number') {
      return { revId: e.revId, revert: null, pRevert: null, invalid: `liftwing ${res.status}: ${text.slice(0, 200)}`, latencyMs, tokensIn: null, tokensOut: null, modelVersion: version };
    }
    return { revId: e.revId, revert: p >= 0.5, pRevert: p, latencyMs, tokensIn: null, tokensOut: null, modelVersion: version };
  }
}

export const BASELINES = ['filter-only', 'filter+temporary', 'liftwing-revertrisk-language-agnostic', 'liftwing-enwiki-damaging'] as const;

export function baselineFor(name: string): Predictor {
  if (name === 'filter-only') return filterOnly;
  if (name === 'filter+temporary') return filterTemp;
  if (name === 'liftwing-revertrisk-language-agnostic') return new LiftWingPredictor('revertrisk-language-agnostic');
  if (name === 'liftwing-enwiki-damaging') return new LiftWingPredictor('enwiki-damaging');
  throw new Error(`unknown baseline "${name}" (one of ${BASELINES.join(', ')})`);
}
