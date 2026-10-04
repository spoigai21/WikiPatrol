import { afterEach, describe, expect, it, vi } from 'vitest';
import { GeminiClient, GroqClient, OllamaClient, QuotaExhausted, RetryLater } from '../src/models/clients.ts';
import { parseAnswer } from '../src/phase5/prompts.ts';

const msgs = { system: 's', user: 'u' };

describe('model clients', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('turn a network failure into a retry, not a stop', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => { throw new TypeError('fetch failed'); }));
    await expect(new OllamaClient('m').complete(msgs)).rejects.toBeInstanceOf(RetryLater);
  });

  it("return Groq's rejected JSON as the model's answer, which then scores as invalid", async () => {
    const body = { error: { message: 'Failed to generate JSON', code: 'json_validate_failed', failed_generation: '{"revert":true,"p_revert":0. nine}' } };
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify(body), { status: 400 })));
    const c = await new GroqClient('openai/gpt-oss-120b', 0, 'key').complete(msgs);
    expect(c.text).toBe('{"revert":true,"p_revert":0. nine}');
    expect(parseAnswer(c.text).ok).toBe(false);
  });

  it("recognise Gemini's daily free-tier cap, reported deep in the error body", async () => {
    const body = { error: { code: 429, message: 'You exceeded your current quota. Please retry in 19h36m21s.', details: [{ violations: [{ quotaId: 'GenerateRequestsPerDayPerProjectPerModel-FreeTier' }] }] } };
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify(body), { status: 429 })));
    await expect(new GeminiClient('m', 0, 'key').complete(msgs)).rejects.toBeInstanceOf(QuotaExhausted);
  });

  it('treat a per-minute 429 as a retry', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: { message: 'Rate limit reached for requests per minute' } }), { status: 429, headers: { 'retry-after': '7' } })));
    await expect(new GroqClient('m', 0, 'key').complete(msgs)).rejects.toBeInstanceOf(RetryLater);
  });

  it('still stop on other request errors', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: { code: 'invalid_api_key' } }), { status: 401 })));
    await expect(new GroqClient('m', 0, 'key').complete(msgs)).rejects.toThrow(/groq 401/);
  });
});
