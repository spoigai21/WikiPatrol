// The test runner resolves CommonJS packages more leniently than Node does at runtime: a named
// import from kafkajs that Node rejects still passes under vitest. Load every module that touches
// kafkajs the way `npm run …` does — a separate Node process through tsx — and require it to work.

import { execFileSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

const MODULES = [
  'src/kafka/topics.ts',
  'src/kafka/stage.ts',
  'src/ingest/kafka-sink.ts',
  'src/labels/kafka-labeller.ts',
  'src/pipeline/stages.ts',
  'src/phase5/runner.ts',
  'src/classifier/classifier.ts',
  'src/ops/health.ts',
  'src/phase9/drift.ts',
];

describe('runtime imports', () => {
  it.each(MODULES)('%s loads under Node', (m) => {
    const out = execFileSync('npx', ['tsx', '-e', `import('./${m}').then(() => console.log('ok'))`], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
    expect(out.trim()).toBe('ok');
  }, 60_000);
});
