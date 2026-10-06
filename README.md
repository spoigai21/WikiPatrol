# WikiPatrol

**What does an expensive model actually buy you, when you cannot afford to call it on every edit?**

WikiPatrol watches English Wikipedia's live edit stream, predicts which edits will be reverted within
72 hours, and measures what each step of a cheap-to-expensive model ladder is worth — in recall, in
latency, and in dollars per thousand edits at published list prices.

> **The finding (2026-10-06):** on English Wikipedia edits, the expensive cloud model (Gemini 3.8
> Flash) was the best judge of which edits get reverted — F1 0.39 against 0.33 for a small local
> model — for about **$0.22 per 1,000 edits ($17–28 a day)** at list prices. But the planned
> cost-saving ladder, which only asks the expensive model when the local one is unsure, **did not
> beat the local model on its own**: the local model cannot tell when it is unsure (its confidence
> is close to random), so it almost never handed anything up. A cheap model you want to route on has
> to be good at knowing what it does not know — this one was not.

## The short version

**The problem.** People edit Wikipedia all the time, and some of those edits get undone — "reverted" —
because they were vandalism, mistakes, or arguments. You could ask an AI model to look at every edit
and guess which ones will be undone. But good AI models cost money for every question you ask, and
English Wikipedia gets about 100,000 edits a day. So the real question is: **how little of the
expensive model can you get away with?**

**The idea.** Don't send everything to the expensive model. First, throw out edits that are
obviously fine — bots, and people who have been editing for a long time. Then let a small free model
running on a laptop look at what is left. Only when the small model is unsure, ask the expensive
one. That chain is the "ladder". The project measures how much each step catches and what it costs.

**What it found.**

- *An undone edit is usually not vandalism.* In a sample of 100 edits, about 4 out of 5 undone edits
  were honest edits caught up in someone else's cleanup. So the project predicts "will this be
  undone?", and says so plainly, instead of claiming to catch vandals.
- *The free first step does most of the work.* Just skipping bots and long-time editors removes
  two-thirds or more of all edits, for nothing. It misses about one in five of the edits that later
  get undone — mostly the honest ones. It also cuts the cloud bill to a third.
- *The expensive model is the better judge.* Gemini 3.8 Flash is right about 3 times in 10 when it
  says an edit will be undone; the small local model about 2 in 10, because it flags most of
  everything. The cloud model's confidence also means something; the local model's does not.
- *The ladder idea failed here, for a clear reason.* The plan was to ask the expensive model only
  when the cheap one was unsure. But the cheap one is never usefully unsure — its confidence barely
  tracks whether it is right — so the ladder ended up being the cheap model on its own. The rule for
  the ladder was fixed before seeing the final results, and is reported as it came out.
- *The pipeline needs a waiting room.* A rate-limited AI model falls behind even the quietest stream
  of edits, so edits queue up in Kafka until the model gets to them — and nothing is lost or counted
  twice while they wait.
- *Guesses made at the start were often wrong — on purpose, on the record.* Predictions were saved
  before any results existed; five of them turned out wrong, including that the cloud model would
  catch more undone edits (it caught fewer, but was right more often).

**How to trust it.** The test edits were frozen before any model saw them. The questions given to
the models were saved and locked before the first one ran. Each model got one try at the final test
set, and the ladder's rules were chosen on separate practice edits, so nothing was tweaked until it
looked good. Every number links to the file it came from, and everything that went wrong is written
up in [`POSTMORTEM.md`](POSTMORTEM.md).

## What was measured, in detail

**1. Most reverts are not vandalism — so this predicts reverts, and says so.**
In a sample of 100 edits (50 reverted, 50 kept), **78% of the reverted edits were not vandalism**
(95% CI 64–88%); none of the kept ones were (0–7%). A revert-trained "vandalism detector" would be
grading itself against the wrong answer, so the target is named for what the label is.
Caveat: that sample was labelled by an AI model blind to revert status, not by hand.
[`results/phase0/label-noise.json`](results/phase0/label-noise.json) · `DECISIONS.md` D9

**2. A free filter removes most of the volume before any model runs.**
Dropping bots and accounts at least 30 days old with at least 500 edits (Wikipedia's own trust
level) removes **67–87% of edits** and loses **13–24% of the edits later reverted**, over three
measured hours, labelled by the project's own stream labeller. The reverts it gives up look like
good-faith ones: in the sample above, all 12 reverted edits it dropped were not vandalism, and all 10
vandalism edits passed through.
[`results/phase3/filter-eval-stream-labels.json`](results/phase3/filter-eval-stream-labels.json) · D10

**3. The model grid — revert prediction on 1,000 sealed edits that pass the filter.**
16.7% of them were reverted within 72 hours. Wilson 95% intervals; each model shown with its best
prompt on the sealed set (all three prompts in the full table):

| Configuration | Precision | Recall | F1 | Flags |
|---|---|---|---|---|
| Filter only (flag everything it passes) | 16.7% [14.5, 19.1] | 100% | 0.286 | 100% |
| Filter + flag temporary accounts | 24.2% [20.5, 28.4] | 66.5% [59.0, 73.2] | 0.355 | 45.8% |
| LiftWing revert-risk (Wikimedia) | 22.2% [19.2, 25.5] | 88.6% [82.9, 92.6] | 0.355 | 66.6% |
| LiftWing `damaging` (ORES) | 40.5% [32.2, 49.4] | 29.3% [23.0, 36.6] | 0.340 | 12.1% |
| `gemma3:4b`, local | 21.4% [18.2, 24.9] | 74.9% [67.8, 80.8] | 0.332 | 58.5% |
| Gemini 3.5 Flash-Lite | 30.0% [25.0, 35.5] | 51.5% [44.0, 59.0] | 0.379 | 28.7% |
| **Gemini 3.8 Flash** | **31.0% [26.0, 36.6]** | 53.9% [46.3, 61.3] | **0.394** | 29.0% |

[`results/phase5/grid-sealed.md`](results/phase5/grid-sealed.md) · D12

**4. Does the model know when it is wrong?** Only the cloud ones.

| Model | Calibration error (lower is better) | AUROC (ranking; 0.5 = chance) |
|---|---|---|
| `gemma3:4b` (local) | 0.40–0.54 | 0.60–0.63 |
| Gemini 3.5 Flash-Lite | 0.18–0.20 | 0.69–0.70 |
| Gemini 3.8 Flash | 0.16–0.20 | 0.72 |
| LiftWing revert-risk / damaging | 0.46 / 0.09 | 0.74 / 0.72 |

[`results/phase6/sealed/`](results/phase6/sealed/) · D18

**5. The ladder did not beat the local model alone.** Chosen on 200 separate dev edits by rules
written down first (D17): escalate to 3.8 Flash when the local model's confidence falls in [0.6, 0.7).

| Policy | Precision | Recall | F1 | Sent to cloud | $ per 1,000 edits |
|---|---|---|---|---|---|
| Heuristics only | 16.7% | 100% | 0.286 | 0% | $0 |
| Local only | 20.0% | 72.5% | 0.314 | 0% | $0 |
| Cloud only (3.8 Flash) | 30.6% | 53.9% | 0.391 | 100% | $0.22 |
| Ladder | 20.1% | 71.3% | 0.313 | 2.6% | $0.005 |

The rule aimed to keep 95% of the cloud model's recall as cheaply as possible — but the local model
already has *more* recall (it flags most edits), so the rule escalated almost nothing. The cloud
model wins on precision, and the local model's confidence is too weak to say which edits to send
up. Costs at list prices dated 2026-10-04; $ per 1,000 classifiable edits, after the filter.
[`results/phase7/ladder.md`](results/phase7/ladder.md) · D17

*Every number in sections 2–5 is about predicting reverts. Read it beside the 78% in section 1.*

**6. Kafka earns its place through backpressure, not volume.**
English Wikipedia's classifiable stream is about 1.1 edits a second — a file could carry it. But a
model tier at a free-tier pace of 10 edits a minute fell **514 edits behind in 20 minutes** of live
traffic; at full speed the backlog drained, and every one of 1,565 messages was handled exactly once.
That backlog is also the signal the classifier autoscales on.
[`results/phase2/backpressure-2026-10-04T0534Z.svg`](results/phase2/backpressure-2026-10-04T0534Z.svg) · D1, D13

**7. The predictions made before any of this were often wrong — on the record.**
Predictions were git-tagged (`phase0-predictions`) before any result existed. Wrong: the revert rate
(predicted 10–25%, measured 6.2%), reverted-but-not-vandalism (under 20% → 78%), how much the filter
removes (30–60% → 67–87%), the recall the cloud model adds (+10–25 points → 17–36 points *fewer*),
and whether confidence routes (yes → not for the local model).
[`PREDICTIONS.md`](PREDICTIONS.md) · scorecard in [`DECISIONS.md`](DECISIONS.md)

**Still running:** the 24-hour autoscaling run (Phase 8), and the first labels from the live
cluster's own log (they need it to be 72 hours old).

## How it works

![Architecture](docs/architecture.svg)

- **Ingest** — Server-Sent Events from Wikimedia, resumed by event id across the stream's
  frequent disconnects, deduplicated, written raw to Kafka (Redpanda) and kept forever.
- **Stages** — parse, enrich (account age and edit count, looked up once and recorded), filter.
  Each reads one topic and writes one; the deterministic ones replay byte-for-byte from any offset.
- **Labels** — computed from the log itself: an edit is labelled once 72 hours of *event time*
  have passed, from `mw-reverted` tag changes. Checked against the Wikipedia API: 11,490 of 11,491
  edits agree.
- **Classifier** — scales from 1 to 6 replicas on its Kafka consumer lag (KEDA), over a
  6-partition topic.
- **Evaluation** — offline, on replayed hours frozen as checksummed snapshots. Prompts were
  git-tagged before any ran; the sealed set is scored once per configuration; no model grades
  another — the label is the revert.
- **Drift** — a nightly job reruns 50 fixed sealed edits per configuration and alerts when answers
  change; a deliberately degraded configuration trips it.

## Run it

```bash
docker compose up -d --build     # Redpanda, ingester, pipeline stages, classifier
curl localhost:8080/readyz       # pipeline health
```

```bash
npm ci
npm test                                      # unit tests
docker compose up -d redpanda && npm run test:kafka   # plus the Kafka tests
```

Kubernetes: `deploy/k8s/` (with KEDA). `deploy/ci-smoke.sh` provisions a `kind` cluster, deploys,
checks live edits reach predictions, and tears it down — CI runs it on every push.

Evaluation: `npm run phase5:run`, `phase5:score`, `phase6:calibration`, `phase7:ladder`, `phase9:drift`
(model API keys in a git-ignored `.env`).

## Honest limits

- **Revert prediction, not vandalism detection** — see finding 1.
- **ORES/LiftWing is the baseline, not the target.** It has more resources and better data; this
  project does not claim to beat it. Its question is cost and routing.
- **English Wikipedia only.** Labels arrive 72 hours late; an edit reverted after that counts as kept.
- **A local cluster, not production.** No served users, no live traffic at scale.
- **List prices, not invoices.** Every cost figure is computed from dated published prices
  ([`results/prices/`](results/prices/)). Most runs used free tiers; the Gemini runs were finished on
  the paid tier for about $3.13 at list price (D12). Groq `gpt-oss-120b` was left unfinished on its
  free tier and is not in the table.

## Documents

[`SPEC.md`](SPEC.md) — the plan · [`DECISIONS.md`](DECISIONS.md) — every choice and the number
behind it · [`POSTMORTEM.md`](POSTMORTEM.md) — what went wrong and what changed ·
[`PREDICTIONS.md`](PREDICTIONS.md) — written before any result
