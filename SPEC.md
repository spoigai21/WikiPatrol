# WikiPatrol — what does an expensive model actually buy you, when you cannot afford to call it?

*(Named 2026-10-02. "Patrol" is the real Wikipedia term — editors who watch the live edit feed
are doing "recent changes patrol" — so the name says what it does without overclaiming. It does
NOT moderate: it classifies and scores, and never edits Wikipedia. Rejected: WikiModerator, which
implies it acts, and WikiCurator, which is the wrong verb entirely.)*

**One line:** watch Wikipedia's live edit firehose, predict which edits will be reverted within
72 hours, and measure what each tier of a cheap-to-expensive model ladder is actually worth — in accuracy, in latency,
and in dollars per thousand edits at list prices.

**The question:** you cannot call a frontier model on every edit — about 98,000 a day on English
Wikipedia articles alone, non-bot (Phase 0, `DECISIONS.md` D1). So where exactly do you put the
cutoffs, and what does each step up the ladder buy?

---

## Why each component is here

Kafka and Kubernetes are easy to add as decoration — a few hundred model calls a night is a `for`
loop and a cron job, and anyone reviewing it would say so in ten seconds. Every component here has
to be forced by a constraint that exists whether you like it or not, and Phase 0 has to confirm
that the constraint is real:

| Component | The constraint that forces it |
|---|---|
| **Kafka** | **Not volume:** Phase 0 measured ~1.1 non-bot enwiki article edits/s, below the 2/s line (see `DECISIONS.md` D1). Kept for what volume does not provide: the classifier tiers are rate-limited and far slower than the feed (backpressure), **consumer lag is the autoscaling signal**, independent consumer groups (live classifier, nightly re-eval, dashboard), and **replay is the experiment** — every configuration must see identical events by offset. |
| **Kubernetes** | Edit volume swings between US daytime and overnight, so **HPA on consumer lag is a real trigger**, not a staged load test. *Phase 0 measured only ~1.5x hour to hour (`DECISIONS.md` D8) — weaker than "several-fold"; how Phase 8 argues this is open.* The ingester must survive forced disconnects, which Phase 0 saw every 2–17 minutes (D3). The nightly re-eval is a CronJob. Classifier and ingester scale independently. |
| **The model ladder** | Economically mandatory, not a study. At ~1.1 classifiable edits/second on enwiki alone (Phase 0, D1), calling a frontier model on every one is a standing daily bill; the dollar figure is measured in Phase 7, not assumed here. The routing work is what makes the system exist at all. |
| **TypeScript / Node** | The services. A long-lived streaming client and async I/O-bound consumers are what Node is good at, and Zod gives a typed boundary on untrusted event JSON. |

---

## What is verified, and what is not

**Verified** (`wikitech.wikimedia.org`, 2026-10-02):

- `stream.wikimedia.org`, the **`recentchange`** stream, **Server-Sent Events** over plain HTTP
- **No API key, no auth, no payment card.** Free.
- **Replay** via `since` or `Last-Event-ID`, with **7–31 days** of history
- **Connections are terminated at 15 minutes** by WMF's HTTP layer; clients must reconnect automatically.
  Observed in Phase 0: cuts arrive more often than that (5 in one hour, 2–17 min apart)
- Wikimedia runs it on Kafka in its own infrastructure and offers it to external tool developers

**Not verified — Phase 0 settles both before a line of system code is written:**

1. **The actual event rate — for the scope actually classified.** The ~20/second figure is the
   global stream across every wiki; English Wikipedia alone is a fraction of that. The Kafka
   argument has to hold for the enwiki rate, not the global one (or the ingester captures all
   wikis and only classification is enwiki-scoped — decide this from the number). If it is
   2/second, Kafka is not justified and the honest move is to say so and stop. Measure it.
2. **Whether revert tags are reliably available, and where.** `mw-rollback`, `mw-undo` and
   `mw-manual-revert` sit on the *reverting* edit, so they can arrive in `recentchange`. But
   `mw-reverted` is added to the *original* edit after the fact, and a tag added later does not
   produce a new `recentchange` event. Check whether `mediawiki.revision-tags-change` (or the
   Action API) is needed to attach the label to the edit that was reverted. The free-label
   mechanism depends on it. Two documentation fetches did not confirm it; go and look.

---

## Honest limits, written before any results

- **The target is "reverted within 72h", not "vandalism"** (`DECISIONS.md` D9). Edits get
  reverted over content disputes, style disagreements and good-faith errors, and Phase 0 found
  most reverts are not vandalism (78% of reverted edits in the sample). So this project predicts
  reverts — what the label actually measures, and what ORES's `damaging` model also learns from —
  and never calls itself a vandalism detector. The Phase 0 noise estimate says how far the two
  differ and is quoted beside every result, so nobody reads "revert recall" as "vandalism recall".
- **ORES / LiftWing already does this**, with more resources and better data. **Do not claim to
  beat it on accuracy.** It is the baseline. The contribution is the cost-and-routing analysis,
  which is a different question and one nobody publishes.
- **Delayed labels.** You classify now; the truth arrives minutes to hours later. Anything
  unreverted after the window is *assumed* good, which is survivorship bias. State the window.
- **English Wikipedia only**, at least to start. Do not generalise across languages.
- **Local cluster, not production.** See "Describe it honestly" in Phase 8.

---

## Phase 0 — measure and predict before building anything

Nothing is built until this is done. Predict first, then measure, so the prediction can be wrong.

- connect to the stream for **one hour** and count: events/second, by wiki, by anonymous vs
  registered vs bot, and the diurnal pattern if you can sample twice — Phase 8's HPA argument
  depends on the swing being real, so sample at least one US-daytime and one overnight hour
- confirm revert tags are present and queryable, and from which stream (see above)
- hand-label **100 edits** and measure how often "later reverted" agrees with "actually vandalism".
  Stratify it — e.g. 50 that were reverted and 50 that were not — because a uniform random 100 at
  a low vandalism base rate contains only a handful of vandalism cases and cannot estimate the
  noise in either direction. Report both disagreement rates, not one blended number
- **git-tag predictions before any run:** the vandalism base rate, the cheap model's accuracy, how
  much the expensive model adds, what fraction of traffic the heuristics can safely drop, and how
  many of these you expect to be wrong

**Done when:** the numbers exist and the Kafka decision is justified by one of them. **If the rate
does not justify Kafka, write that down and change the project.** That finding is worth more than
a system built on a false premise.

## Phase 1 — the ingester, in TypeScript and Node

- SSE client that reconnects with `Last-Event-ID` after the 15-minute cut
- writes **raw events only** to Kafka — no parsing, no model, no opinions
- dedupes on the event's own id/offset across reconnects, so reconnecting after the cut is idempotent
- strict TS, Zod at the boundary, Vitest from the first commit
- topic retention set so captured events are never aged out (Redpanda defaults will delete
  them); the dev and sealed offset ranges must still exist in Phase 5 and Phase 9

**Done when:** a forced disconnect loses nothing and duplicates nothing, proven by a test that
kills the connection mid-stream and diffs the output.

## Phase 2 — Kafka, and proving it earns its place

- one raw topic, one scored topic, one dead-letter topic; consumer groups; explicit offsets
- **replay from an arbitrary offset reproduces byte-identical output** for every deterministic
  stage (raw capture, parsing, the Phase 3 filter). Model outputs are not guaranteed
  deterministic even at temperature 0 — replay guarantees identical *inputs* to every
  configuration, and run-to-run model variance is measured, not assumed away
- **backpressure demo:** run a consumer deliberately slower than the feed, show lag growing and
  nothing lost — this is the artifact that justifies the whole component

**Done when:** the replay test passes and the lag graph exists.
*(Done: `test/kafka-replay.test.ts`; lag graph `results/phase2/backpressure-2026-10-04T0534Z.svg` —
514 messages behind after 20 minutes at 10/min, drained, every offset exactly once. `DECISIONS.md` D13.)*

## Phase 3 — the free filter, before any AI

Most edits are bots or long-established users and are obviously fine. Cheap, explainable rules:
is the user a bot, are they autoconfirmed, how large is the diff, does it touch references.
*(Built: bot flag plus Wikipedia's own account-trust thresholds; diff size and references need
the diff text and are left to the models — `DECISIONS.md` D10.)*

- measure **what fraction of traffic survives the filter**
- measure **what fraction of later-reverted edits the filter throws away** — the recall you are
  paying for the cost saving. This needs labels: the revert labels from Phase 4. The Phase 0
  sample is too small for this (50 reverted rows) but gives an early read on how much true
  vandalism the filter drops, reported separately. Build the filter here; the M% is finalised after
  Phase 4.
- filter rules are tuned on the development range only, like everything else

**Done when:** you can state the trade: "the filter removes N% of volume and loses M% of
later-reverted edits" — M measured against Phase 4 labels, with the label-noise estimate beside it.
*(Done 2026-10-06: removes 67–87% of edits, loses 13–24% of later-reverted edits, on Phase 4 stream
labels over three hours — `DECISIONS.md` D10.)*

## Phase 4 — labels, for free, from reverts

- join each edit to its later revert status within a stated window (72h; `DECISIONS.md` D4)
- store `(edit, prediction, label, latency, config)` as the result table
- the sample from Phase 0 tells you how noisy this label is; carry that number everywhere

**Done when:** labels arrive automatically and the noise estimate is published beside them.
*(Built: `npm run labeller`, `wiki.raw` → `wiki.labels`; validated against the Action API on
11,491 edits — `DECISIONS.md` D11.)*

## Phase 5 — the grid

Three prompts x three models — **local Ollama**, **Gemini free tier**, **Groq free tier** — scored
against revert labels on a **sealed replay set** of captured edits.

- split: develop prompts on one offset range, score **once** on a sealed range. Both ranges are
  fixed in time **after** the label window has closed on them, and the sealed range is exported
  to a committed, checksummed snapshot file so it survives a cluster rebuild
- all three prompts written and tagged **before** any of them runs. Each asks the question the
  label answers — *will this edit be reverted?* — not *is this vandalism?*
- **no judge model.** The label is the revert. An LLM grading an LLM is circular.
- baselines in the table: the Phase 3 heuristics alone, and ORES/LiftWing if reachable

**Done when:** the table exists, with baselines as rows, and you know which Phase 0 predictions
were wrong.
*(Done 2026-10-06: local `gemma3:4b`, Gemini 3.5 Flash-Lite and Gemini 3.8 Flash × 3 prompts, plus
four baselines, on the sealed set — `results/phase5/grid-sealed.md`. Groq left unfinished on its free
tier; the Gemini runs finished on the paid tier, $3.13 at list price. D12; predictions scored in the
DECISIONS scorecard.)*

## Phase 6 — does the model know when it is wrong?

- bucket predictions by stated confidence; plot accuracy per bucket
- report **expected calibration error**, not only accuracy
- check whether calibration differs for anonymous vs registered editors

**Done when:** you can say yes or no on whether confidence is usable as a routing signal, with the
curve to prove it. A "no" is a real result and Phase 7 then has to route on something else.
*(Done: yes for the Gemini models, no for the local model — D18, `results/phase6/sealed/`.)*

## Phase 7 — the ladder: the economic core

heuristics → local model → cloud model, escalating only on uncertainty. Thresholds tuned on the
development range only.

Compare four policies on the sealed range: **heuristics only · local only · cloud only · ladder**.
Report for each: recall, precision, **dollars per 1,000 edits at dated list prices**, and p95
latency.

**Note on the free tiers:** the cloud providers' rate limits behave exactly like a cost ceiling,
which is a free and faithful simulation of the real constraint. But **report money from published
list prices, dated** — writing "$0 because free tier" would destroy the finding.

**Done when:** you can state the trade honestly, in the shape *"the ladder held Y% of
cloud-only recall at Z% of cloud-only cost"* — or that it did not beat local-only, which is equally worth publishing.
*(Done: the pre-registered ladder did not beat local-only; the cloud model alone had the best F1 at
$0.22 per 1,000 edits — D17, `results/phase7/ladder.md`.)*

## Phase 8 — Kubernetes, driven by the real load

- Deployments for ingester and classifier, scaled independently
- **HPA on consumer lag**, demonstrated against the genuine diurnal swing rather than a synthetic
  load test. This needs the ingester and classifier up for at least 24 hours straight; the
  classifier tier during that run is the heuristics plus a small local model, not the cloud APIs
- liveness and readiness probes that actually fail when the service is sick
- `kind` in GitHub Actions: CI provisions a cluster, applies manifests, waits for readiness, runs
  smoke tests, tears it down — on every push

*(Built: `deploy/k8s/`, `deploy/ci-smoke.sh`, `.github/workflows/ci.yml`; the classifier scales 1 → 6 on
lag over a 6-partition `wiki.scored`; CI validated on a local `kind` cluster — `DECISIONS.md` D14.
The 24-hour run: done 2026-10-08, 24 hours with no gaps or restarts; the replicas followed the
kept-edit load at r = 0.92, not the total feed (r = 0.18) — D16 results.)*

### Describe it honestly

- **True:** *"Ingester and classifier on Kubernetes, autoscaled on Kafka consumer lag, with a CI
  job that provisions a throwaway cluster and verifies the deployment on every push."*
- **Not true here:** production · managed cluster · live traffic at scale · served users.

## Phase 9 — drift

- nightly CronJob re-runs the **sealed** range against every configuration
- results stored as a time series; **a regression trips an alert**
- providers change models silently behind the same name; this is the system noticing
- these reruns are **monitoring, not tuning**: they repeat configurations that were already
  scored, unchanged. If drift prompts a prompt or model change, that is a new configuration — it
  is developed on the dev range and gets its own single score on the sealed range, never tuned
  against nightly sealed results

**Done when:** a deliberately degraded config is caught by the alert without you looking.
*(Built and shown: a 50-edit subset per configuration, because a nightly full rerun does not fit
the free tiers; a 30%-degraded config tripped the alert — `DECISIONS.md` D15. All 11 configurations, the Gemini
ones included, now have measured thresholds. Not yet done: scheduling the CronJob on the cluster
(`deploy/k8s/optional/drift-cronjob.yaml`, which needs the cluster created with `kind-local.yaml`).)*

## Phase 10 — ship it and write the postmortem

- a public dashboard or feed someone other than you actually watches. A laptop cluster cannot
  keep it live, so the dashboard reads published results (and, if a free always-on host is
  found, a live feed); it must say plainly when it is showing a replay rather than live data
- README carries: the Phase 5 table, the Phase 6 calibration curve, the Phase 7 policy comparison,
  the label-noise estimate, and the dated price table
- a write-up naming **where the model failed, how you noticed, and what you changed**

*(Started 2026-10-04: `README.md` leads with what is measured and marks the headline as pending;
`POSTMORTEM.md` is written as it happens; `docs/architecture.svg`; a static dashboard built from
the committed results — `npm run dashboard:build`, served by Vercel via `vercel.json`, labelled as
replayed data. The headline and the cost table are in (2026-10-06); the Phase 8 and Phase 9 results
joined the README and dashboard on 2026-10-07. No video, by the owner's
decision (2026-10-04); the plain-language write-up lives in the README instead.)*

---

## Stack

**Target cost: $0, and no payment card anywhere.**

| Layer | Choice | Cost |
|---|---|---|
| Source | **Wikimedia EventStreams** `recentchange` (SSE, no auth) | **$0** |
| Language / runtime | **TypeScript** strict, **Node 22** | free |
| Transport | **Redpanda** (Kafka API) in-cluster | **$0** |
| Orchestration | **Kubernetes** — `k3d` locally, `kind` in GitHub Actions | **$0, no card** |
| Results store | **Neon** or **Supabase** Postgres free tier — working store; every reported number is also exported to a committed result file | **$0** |
| Cheap model | **local Ollama** — no rate limit | **$0** |
| Expensive model | **Gemini** and **Groq** free tiers — rate limits act as the cost ceiling | **$0** |
| Scheduling | Kubernetes **CronJob** | free |
| CI | Vitest + GitHub Actions | free |
| Dashboard | **Vercel** Hobby tier, reading published results | **$0, no card** |

**Why this is affordable at all:** the stream is captured into Kafka with **zero model calls**, and
every experiment runs offline by replaying stored offsets. Model spend is a bounded function of
how many experiments you run, not of the live edit rate.

**Laptop warning:** k3d + Redpanda + Postgres + two Node services + Ollama is real load. Keep the
local model small and do not run it continuously, with two exceptions that are scheduled, not
standing: the 24-hour Phase 8 diurnal run, and the Phase 9 nightly CronJob (which only needs the
laptop up at its scheduled time).

## Measurement discipline

- Nothing is reported that is not reproducible from a committed result file.
- Every cost figure carries the date of the price table it came from.
- The sealed offset range is scored **once per configuration**, never iterated on.
- Phase 0's predictions are quoted in the README beside what happened, right or wrong.
- Every accuracy number is called what it is — revert recall/precision — with the Phase 0
  label-noise estimate beside it.

## Deliverables that are not code — these do more work than the code does

Most people who look at this will never open the code. Build these deliberately.

| Artifact | Why it earns its keep |
|---|---|
| **A dashboard** on Vercel: the feed arriving, each edit's ladder decision, the running cost counter | *"Here it is running"* beats every description, and it is the cheapest of these to host. Label replayed data as replay. |
| **The write-up** — the cost curve, the calibration plot, the honest negatives | This is the thing that circulates and gets quoted. |
| ~~**A 2-minute video** of the feed being classified~~ | Dropped (owner's decision, 2026-10-04): the README's plain-language write-up does this job. |
| **`DECISIONS.md`** — every architectural choice with the measured number that forced it | **The single highest-value document in the repo.** It turns *"why Kafka?"* from a weak point into the strongest answer: "the connection dies every 15 minutes, the classifier runs at N/sec against a feed at M/sec, and every experiment has to replay identical events." |
| **`POSTMORTEM.md`** — where the model failed, how you noticed, what you changed | Write it as you go, not at the end. |
| **An architecture diagram** | One image. Goes in the README and the write-up. |
| **`docker compose up`** path alongside the Kubernetes one | Anyone can run it in 60 seconds without a cluster. Reproducibility is a quality signal in itself. |

**Lead the README with the finding, not the architecture.** Almost every project README opens with
"a system for X built with Y" and loses the reader. Open with the measured number, in the shape:
*"A frontier model costs $X/day to run on Wikipedia's edit feed. A three-tier ladder held Y% of its
recall for $Z."* Then explain how. The X, Y and Z come from Phase 7 at dated list prices — no
figure goes in before it is measured.

**Give it one quotable sentence.** One number, one comparison.

## Order of work

The phases are numbered by dependency, not by urgency. A measured result in three weeks is worth
more than a perfect system in three months.

**Minimum path: Phases 0 → 1 → 3 → 4 → 5.** That is the ingester, the free filter, revert labels,
and the grid — which produces the table. Phase 2's formal replay and backpressure proofs can
follow, but Phase 5's sealed range depends on replaying fixed offsets, so from Phase 1 onward the
raw topic must already keep its offsets and its data (see Phase 1 retention). Phase 3's revert-loss
number is completed once Phase 4's labels exist.

**Then, in order of marginal value:** Phase 7 (the ladder — the strongest single result), Phase 6
(calibration), Phase 8 (Kubernetes), Phase 9 (drift). Phase 7 routes on confidence only if Phase 6
says confidence is usable; if Phase 7 is built first, treat its routing signal as provisional
until Phase 6 is done.

**Do not do Phase 8 first.** Infrastructure with no result attached reads as a tutorial.

## Claims, and when they can be made

**Nothing is claimed until Phase 5 produces the table.** After Phase 7 there are three headline
results — the ladder's cost result, the calibration finding, and the filter trade-off — and after
Phase 9 a fourth about catching silent model drift. All measured, none of them claiming
production.
