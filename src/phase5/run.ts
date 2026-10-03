// Fill one cell of the grid.
//
//   npm run phase5:run -- --set dev --model ollama:gemma3:4b --prompt p1-plain
//   npm run phase5:run -- --set sealed --model groq:<model> --prompt p2-guide --rpm 30
//   npm run phase5:run -- --set sealed --baseline liftwing-revertrisk-language-agnostic

import { args, log } from '../phase0/cli.ts';
import { clientFor } from '../models/clients.ts';
import { baselineFor, LlmPredictor, type Predictor } from './predictors.ts';
import { PROMPT_IDS, type PromptId } from './prompts.ts';
import { loadSet, runPredictor } from './runner.ts';

const opts = args({
  set: { type: 'string' },
  model: { type: 'string' },
  prompt: { type: 'string' },
  baseline: { type: 'string' },
  rpm: { type: 'string' },
  limit: { type: 'string' },
});
if (!opts.set) throw new Error('--set is required');

let predictor: Predictor;
if (opts.baseline) predictor = baselineFor(String(opts.baseline));
else {
  if (!opts.model || !opts.prompt) throw new Error('--model and --prompt (or --baseline) are required');
  if (!(PROMPT_IDS as readonly string[]).includes(String(opts.prompt))) throw new Error(`--prompt must be one of ${PROMPT_IDS.join(', ')}`);
  predictor = new LlmPredictor(clientFor(String(opts.model), opts.rpm ? Number(opts.rpm) : undefined), opts.prompt as PromptId);
}

let edits = loadSet(String(opts.set));
// --limit is for smoke tests on dev; the sealed set is always run whole.
if (opts.limit) {
  if (opts.set === 'sealed') throw new Error('--limit is not allowed on the sealed set');
  edits = edits.slice(0, Number(opts.limit));
}

const ctl = new AbortController();
process.once('SIGINT', () => ctl.abort());
const r = await runPredictor({ set: String(opts.set), edits, predictor, signal: ctl.signal, log });
log(`${predictor.config} on ${opts.set}: ${r.done}/${r.total}${r.stopped ? ` (stopped: ${r.stopped}; rerun to resume)` : ' complete'}`);
