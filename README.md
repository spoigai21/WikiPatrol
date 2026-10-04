# WikiPatrol

**What does an expensive model actually buy you, when you cannot afford to call it on every edit?**

WikiPatrol watches English Wikipedia's live edit stream, predicts which edits will be reverted within
72 hours, and measures what each step of a cheap-to-expensive model ladder is worth — in recall, in
latency, and in dollars per thousand edits at published list prices.

> **Status (2026-10-04):** the evaluation grid is running on free tiers, which cap it at a few
> hundred calls a day, so the cloud models finish in about a week. The headline — what share of a
> cloud model's recall a ladder keeps, and at what share of its cost — is **not measured yet** and
> is not stated anywhere until it is. Everything below is measured, with its source file.

## What is measured so far

**1. Most reverts are not vandalism — so this predicts reverts, and says so.**
In a sample of 100 edits (50 reverted, 50 kept), **78% of the reverted edits were not vandalism**
(95% CI 64–88%); none of the kept ones were (0–7%). A revert-trained "vandalism detector" would be
grading itself against the wrong answer, so the target is named for what the label is.
Caveat: that sample was labelled by an AI model blind to revert status, not by hand.
[`results/phase0/label-noise.json`](results/phase0/label-noise.json) · `DECISIONS.md` D9

**2. A free filter removes most of the volume before any model runs.**
Dropping bots and accounts at least 30 days old with at least 500 edits (Wikipedia's own trust
level) removes **73–87% of edits** and loses **16–24% of the edits later reverted**. The reverts it
gives up look like good-faith ones: in the sample above, all 12 reverted edits it dropped were not
vandalism, and all 10 vandalism edits passed through.
[`results/phase3/filter-eval.json`](results/phase3/filter-eval.json) · D10

**3. On what the filter lets through, a small local model barely beats flagging everything.**
1,000 sealed edits, 16.7% of them reverted within 72 h. Precision and recall are for *revert*
prediction, with Wilson 95% intervals:

| Configuration | Precision | Recall | Flags |
|---|---|---|---|
| Filter only (flag everything it passes) | 16.7% [14.5, 19.1] | 100% | 100% |
| Filter + flag temporary accounts | 24.2% [20.5, 28.4] | 66.5% [59.0, 73.2] | 45.8% |
| LiftWing revert-risk (Wikimedia) | 22.2% [19.2, 25.5] | 88.6% [82.9, 92.6] | 66.6% |
| LiftWing `damaging` (ORES) | 40.5% [32.2, 49.4] | 29.3% [23.0, 36.6] | 12.1% |
| `gemma3:4b` local, best of three prompts | 21.4% [18.2, 24.9] | 74.9% [67.8, 80.8] | 58.5% |

The local model is badly calibrated — when it says 90%, about 20% are reverted — but it still ranks
reverted edits above kept ones better than chance (AUROC 0.60–0.63), which is what a router needs.
Cloud rows (Groq `gpt-oss-120b`, Gemini 3.5 Flash-Lite) are pending.
[`results/phase5/grid-sealed.md`](results/phase5/grid-sealed.md) ·
[`results/phase6/sealed/`](results/phase6/sealed/) · D12

*Every number in this section is about predicting reverts. Read it beside the 78% above.*

**4. Kafka earns its place through backpressure, not volume.**
English Wikipedia's classifiable stream is about 1.1 edits a second — a file could carry it. But a
model tier at a free-tier pace of 10 edits a minute fell **514 edits behind in 20 minutes** of live
traffic; at full speed the backlog drained, and every one of 1,565 messages was handled exactly once.
That backlog is also the signal the classifier autoscales on.
[`results/phase2/backpressure-2026-10-04T0534Z.svg`](results/phase2/backpressure-2026-10-04T0534Z.svg) · D1, D13

**5. The predictions made before any of this were often wrong — on the record.**
Predictions were git-tagged (`phase0-predictions`) before any result existed. Wrong so far: the
revert rate (predicted 10–25%, measured 6.2%), reverted-but-not-vandalism (predicted under 20%,
measured 78%), and how much the filter removes (predicted 30–60%, measured 73–87%).
[`PREDICTIONS.md`](PREDICTIONS.md) · scorecard in [`DECISIONS.md`](DECISIONS.md)

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

Evaluation: `npm run phase5:run`, `phase5:score`, `phase6:calibration`, `phase9:drift`
(model API keys in a git-ignored `.env`).

## Honest limits

- **Revert prediction, not vandalism detection** — see finding 1.
- **ORES/LiftWing is the baseline, not the target.** It has more resources and better data; this
  project does not claim to beat it. Its question is cost and routing.
- **English Wikipedia only.** Labels arrive 72 hours late; an edit reverted after that counts as kept.
- **A local cluster, not production.** No served users, no live traffic at scale.
- **Free tiers, list prices.** The runs cost nothing; every cost figure is computed from dated
  published prices ([`results/prices/`](results/prices/)), never reported as $0.

## Documents

[`SPEC.md`](SPEC.md) — the plan · [`DECISIONS.md`](DECISIONS.md) — every choice and the number
behind it · [`POSTMORTEM.md`](POSTMORTEM.md) — what went wrong and what changed ·
[`PREDICTIONS.md`](PREDICTIONS.md) — written before any result
