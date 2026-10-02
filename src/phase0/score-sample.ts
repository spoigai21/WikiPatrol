// Phase 0, step 3b: score the hand-labelled sample against revert status.
//
//   npm run phase0:noise

import { readFileSync } from 'node:fs';
import { args, log, writeJson } from './cli.ts';
import { parseCsv } from './csv.ts';
import { scoreNoise, type Judgement, type LabelledRow } from './sample.ts';

const opts = args({
  sheet: { type: 'string', default: 'labels/sample.csv' },
  key: { type: 'string', default: 'results/phase0/sample-key.json' },
});

const key = JSON.parse(readFileSync(String(opts.key), 'utf8')) as {
  population: { revertRate: { rate: number } };
  revertWindowHours: string;
  rows: { revId: number; reverted: boolean }[];
};
const revertedById = new Map(key.rows.map((r) => [r.revId, r.reverted]));

const rows: LabelledRow[] = [];
const problems: string[] = [];
for (const r of parseCsv(readFileSync(String(opts.sheet), 'utf8'))) {
  const v = r.vandalism?.toLowerCase();
  const reverted = revertedById.get(Number(r.rev_id));
  if (reverted === undefined) problems.push(`row ${r.row}: rev ${r.rev_id} not in key`);
  else if (v !== 'y' && v !== 'n' && v !== 'u') problems.push(`row ${r.row}: vandalism must be y/n/u, got "${r.vandalism}"`);
  else rows.push({ reverted, vandalism: v as Judgement });
}
if (problems.length) {
  for (const p of problems) log(p);
  log(`${problems.length} rows unlabelled or invalid; finish the sheet first`);
  process.exit(1);
}

const result = {
  scoredAt: new Date().toISOString(),
  revertWindowHours: key.revertWindowHours,
  ...scoreNoise(rows, key.population.revertRate.rate),
};
writeJson('results/phase0/label-noise.json', result);
log(
  `reverted but not vandalism: ${result.revertedNotVandalism.rate} [${result.revertedNotVandalism.low}, ${result.revertedNotVandalism.high}]; ` +
    `kept but vandalism: ${result.keptButVandalism.rate} [${result.keptButVandalism.low}, ${result.keptButVandalism.high}]`,
);
