// Resume every unfinished sealed-set run of the grid (D12), one provider per lane, lanes in
// parallel. Finished configurations are skipped, never rerun. Safe to run once a day until the
// free-tier runs are all complete, then `npm run phase5:score`.
//
//   npm run phase5:resume

import { log } from '../phase0/cli.ts';
import { clientFor } from '../models/clients.ts';
import { LlmPredictor } from './predictors.ts';
import { PROMPT_IDS } from './prompts.ts';
import { loadSet, readRun, runPath, runPredictor, SEALED } from './runner.ts';

// Fixed in D12. rpm keeps each lane under the provider's per-minute limit.
const LANES: { model: string; rpm?: number }[] = [
  { model: 'ollama:gemma3:4b' },
  { model: 'groq:openai/gpt-oss-120b', rpm: 10 },
  // Gemini runs on the paid tier and is run directly (D12, 2026-10-06), not in this free-tier loop.
];

const edits = loadSet(SEALED);
const ctl = new AbortController();
process.once('SIGINT', () => ctl.abort());

await Promise.all(
  LANES.map(async ({ model, rpm }) => {
    for (const prompt of PROMPT_IDS) {
      const predictor = new LlmPredictor(clientFor(model, rpm), prompt);
      const done = readRun(runPath(SEALED, predictor.config)).length;
      if (done >= edits.length) {
        log(`${predictor.config}: complete`);
        continue;
      }
      const r = await runPredictor({ set: SEALED, edits, predictor, signal: ctl.signal, log: (m) => log(`[${model}] ${m}`) });
      log(`${predictor.config}: ${r.done}/${r.total}${r.stopped ? ` (stopped: ${r.stopped})` : ' complete'}`);
      // A spent daily quota applies to every prompt on this provider; try again tomorrow.
      if (r.stopped) return;
    }
  }),
);
