// Prompt files and the answer contract. The prompt text lives in prompts/*.txt (frozen by the
// `phase5-prompts` git tag, D12); this module only fills it in and checks what comes back.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { z } from 'zod';
import type { SetEdit } from './build-set.ts';

export const PROMPT_IDS = ['p1-plain', 'p2-guide', 'p3-reason'] as const;
export type PromptId = (typeof PROMPT_IDS)[number];
export const PROMPTS_TAG = 'phase5-prompts';
const DIR = 'prompts';

export interface Messages {
  system: string;
  user: string;
}

const fill = (template: string, values: Record<string, string>) =>
  template.replace(/\{\{(\w+)\}\}/g, (_, k: string) => {
    if (!(k in values)) throw new Error(`template field {{${k}}} has no value`);
    return values[k]!;
  });

function editor(e: SetEdit): string {
  if (e.userClass === 'temporary') return 'temporary account (not logged in)';
  if (e.userClass === 'registered' && e.accountAgeDays !== null && e.editcount !== null) {
    const days = e.accountAgeDays < 1 ? 'less than a day' : `${Math.floor(e.accountAgeDays)} days`;
    return `registered account, ${days} old, ${e.editcount} edits`;
  }
  return 'registered account';
}

/** The edit as the model sees it. Never includes the label. */
export function renderEdit(e: SetEdit, template = readFileSync(`${DIR}/edit.txt`, 'utf8')): string {
  return fill(template, {
    title: e.title,
    kind: [e.isNew ? 'new page' : 'edit to an existing page', e.minor ? 'marked minor' : ''].filter(Boolean).join(', '),
    editor: editor(e),
    size_delta: e.sizeDelta > 0 ? `+${e.sizeDelta}` : String(e.sizeDelta),
    comment: e.comment.trim() || '(none)',
    diff: e.diffUnavailable ? '[the diff is not available]' : e.isNew ? `(new page text)\n${e.diff}` : e.diff || '(no visible text change)',
  }).trimEnd();
}

export function buildMessages(prompt: PromptId, e: SetEdit): Messages {
  const file = readFileSync(`${DIR}/${prompt}.txt`, 'utf8');
  const m = /^### system\n([\s\S]*?)\n### user\n([\s\S]*)$/.exec(file);
  if (!m) throw new Error(`${prompt}.txt must have a "### system" then a "### user" section`);
  return { system: m[1]!.trim(), user: fill(m[2]!, { edit: renderEdit(e) }).trim() };
}

export const Answer = z.object({
  revert: z.boolean(),
  p_revert: z.number().min(0).max(1),
  reason: z.string().optional(),
});
export type Answer = z.infer<typeof Answer>;

/**
 * The model's answer, or why it is not one. Accepts a JSON object on its own or wrapped in a code
 * fence; anything else is invalid. Nothing is guessed from free text.
 */
export function parseAnswer(raw: string): { ok: true; answer: Answer } | { ok: false; error: string } {
  const body = raw.trim().replace(/^```(?:json)?\s*([\s\S]*?)\s*```$/, '$1');
  let json: unknown;
  try {
    json = JSON.parse(body);
  } catch {
    return { ok: false, error: 'not JSON' };
  }
  const p = Answer.safeParse(json);
  return p.success ? { ok: true, answer: p.data } : { ok: false, error: `wrong shape: ${p.error.issues[0]?.message ?? ''}` };
}

/** Files under prompts/ that differ from the tagged version (or the tag is missing). */
export function promptsChangedSinceTag(): string[] {
  const files = [...PROMPT_IDS.map((p) => `${DIR}/${p}.txt`), `${DIR}/edit.txt`];
  const changed: string[] = [];
  for (const f of files) {
    let tagged: string;
    try {
      tagged = execFileSync('git', ['show', `${PROMPTS_TAG}:${f}`], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
    } catch {
      changed.push(`${f} (not in tag ${PROMPTS_TAG})`);
      continue;
    }
    if (tagged !== readFileSync(f, 'utf8')) changed.push(f);
  }
  return changed;
}
