// One interface over the three model tiers (D12). Deterministic settings everywhere they exist:
// temperature 0 and a fixed seed. Usage is recorded for Phase 7's cost at dated list prices.

import type { Messages } from '../phase5/prompts.ts';

export interface Completion {
  text: string;
  /** The model the API says answered (providers can swap models behind a name: Phase 9). */
  modelVersion: string;
  tokensIn: number | null;
  tokensOut: number | null;
  latencyMs: number;
}

export interface ModelClient {
  /** "provider:model", as written in result rows. */
  readonly id: string;
  /** Minimum gap between calls, from the provider's published per-minute limit. */
  readonly minIntervalMs: number;
  complete(m: Messages): Promise<Completion>;
}

/** A refusal the runner must stop on rather than retry (e.g. the day's free quota is spent). */
export class QuotaExhausted extends Error {}
/** A temporary failure worth retrying after `retryAfterMs`. */
export class RetryLater extends Error {
  constructor(message: string, readonly retryAfterMs: number) {
    super(message);
  }
}

const SEED = 42;
const MAX_TOKENS = 400;

async function postJson(url: string, body: unknown, headers: Record<string, string>): Promise<{ status: number; json: unknown; headers: Headers }> {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body) });
  const text = await res.text();
  let json: unknown = text;
  try {
    json = JSON.parse(text);
  } catch {
    /* keep the text */
  }
  return { status: res.status, json, headers: res.headers };
}

function throwForStatus(provider: string, status: number, json: unknown, headers: Headers): void {
  if (status < 400) return;
  const detail = JSON.stringify(json).slice(0, 400);
  if (status === 429) {
    // Daily caps are reported in the error body; per-minute caps clear on their own.
    if (/per.?day|daily|RPD|quota.*day/i.test(detail)) throw new QuotaExhausted(`${provider} daily quota: ${detail}`);
    const after = Number(headers.get('retry-after'));
    throw new RetryLater(`${provider} 429: ${detail}`, Number.isFinite(after) && after > 0 ? after * 1000 : 20_000);
  }
  if (status >= 500) throw new RetryLater(`${provider} ${status}: ${detail}`, 10_000);
  throw new Error(`${provider} ${status}: ${detail}`);
}

export class OllamaClient implements ModelClient {
  readonly id: string;
  readonly minIntervalMs = 0;
  constructor(private readonly model: string, private readonly base = process.env.OLLAMA_HOST ?? 'http://localhost:11434') {
    this.id = `ollama:${model}`;
  }
  async complete(m: Messages): Promise<Completion> {
    const t0 = performance.now();
    const { status, json, headers } = await postJson(`${this.base}/api/chat`, {
      model: this.model,
      messages: [{ role: 'system', content: m.system }, { role: 'user', content: m.user }],
      stream: false,
      format: 'json',
      options: { temperature: 0, seed: SEED, num_predict: MAX_TOKENS },
    }, {});
    throwForStatus('ollama', status, json, headers);
    const r = json as { model: string; message?: { content?: string }; prompt_eval_count?: number; eval_count?: number };
    return { text: r.message?.content ?? '', modelVersion: r.model, tokensIn: r.prompt_eval_count ?? null, tokensOut: r.eval_count ?? null, latencyMs: performance.now() - t0 };
  }
}

export class GeminiClient implements ModelClient {
  readonly id: string;
  constructor(private readonly model: string, readonly minIntervalMs: number, private readonly key = process.env.GEMINI_API_KEY ?? '') {
    if (!key) throw new Error('GEMINI_API_KEY is not set');
    this.id = `gemini:${model}`;
  }
  async complete(m: Messages): Promise<Completion> {
    const t0 = performance.now();
    const { status, json, headers } = await postJson(
      `https://generativelanguage.googleapis.com/v1beta/models/${this.model}:generateContent`,
      {
        systemInstruction: { parts: [{ text: m.system }] },
        contents: [{ role: 'user', parts: [{ text: m.user }] }],
        generationConfig: { temperature: 0, seed: SEED, maxOutputTokens: MAX_TOKENS, responseMimeType: 'application/json' },
      },
      { 'x-goog-api-key': this.key },
    );
    throwForStatus('gemini', status, json, headers);
    const r = json as {
      modelVersion?: string;
      candidates?: { content?: { parts?: { text?: string }[] } }[];
      usageMetadata?: { promptTokenCount?: number; candidatesTokenCount?: number; thoughtsTokenCount?: number };
    };
    const u = r.usageMetadata;
    return {
      text: r.candidates?.[0]?.content?.parts?.map((p) => p.text ?? '').join('') ?? '',
      modelVersion: r.modelVersion ?? this.model,
      tokensIn: u?.promptTokenCount ?? null,
      // Thinking tokens are billed as output.
      tokensOut: u ? (u.candidatesTokenCount ?? 0) + (u.thoughtsTokenCount ?? 0) : null,
      latencyMs: performance.now() - t0,
    };
  }
}

export class GroqClient implements ModelClient {
  readonly id: string;
  constructor(private readonly model: string, readonly minIntervalMs: number, private readonly key = process.env.GROQ_API_KEY ?? '') {
    if (!key) throw new Error('GROQ_API_KEY is not set');
    this.id = `groq:${model}`;
  }
  async complete(m: Messages): Promise<Completion> {
    const t0 = performance.now();
    const { status, json, headers } = await postJson(
      'https://api.groq.com/openai/v1/chat/completions',
      {
        model: this.model,
        messages: [{ role: 'system', content: m.system }, { role: 'user', content: m.user }],
        temperature: 0,
        seed: SEED,
        max_tokens: MAX_TOKENS,
        response_format: { type: 'json_object' },
      },
      { Authorization: `Bearer ${this.key}` },
    );
    throwForStatus('groq', status, json, headers);
    const r = json as { model?: string; choices?: { message?: { content?: string } }[]; usage?: { prompt_tokens?: number; completion_tokens?: number } };
    return {
      text: r.choices?.[0]?.message?.content ?? '',
      modelVersion: r.model ?? this.model,
      tokensIn: r.usage?.prompt_tokens ?? null,
      tokensOut: r.usage?.completion_tokens ?? null,
      latencyMs: performance.now() - t0,
    };
  }
}

/** "ollama:gemma3:4b", "gemini:<model>", "groq:<model>"; `rpm` = the provider's per-minute limit. */
export function clientFor(spec: string, rpm?: number): ModelClient {
  const [provider, ...rest] = spec.split(':');
  const model = rest.join(':');
  const gap = rpm ? Math.ceil(60_000 / rpm) : 0;
  if (provider === 'ollama') return new OllamaClient(model);
  if (provider === 'gemini') return new GeminiClient(model, gap);
  if (provider === 'groq') return new GroqClient(model, gap);
  throw new Error(`unknown model provider in "${spec}"`);
}
