# WikiPatrol — what does an expensive model actually buy you, when you cannot afford to call it?

*(Named 2026-10-02. "Patrol" is the real Wikipedia term — editors who watch the live edit feed
are doing "recent changes patrol" — so the name says what it does without overclaiming. It does
NOT moderate: it classifies and scores, and never edits Wikipedia. Rejected: WikiModerator, which
implies it acts, and WikiCurator, which is the wrong verb entirely.)*

**One line:** watch Wikipedia's live edit firehose, decide which edits are vandalism, and measure
what each tier of a cheap-to-expensive model ladder is actually worth — in accuracy, in latency,
and in dollars per thousand edits at list prices.

**The question:** you cannot call a frontier model on 1.7 million edits a day. So where exactly do
you put the cutoffs, and what does each step up the ladder buy?

---

## Why this project and not the last one

The previous draft scored saved job postings. It was honest work, but **Kafka and Kubernetes were
decoration** — 450 model calls a night is a `for` loop and a cron job, and an interviewer would
say so in ten seconds. Every component here is forced by a constraint that exists whether you like
it or not:

| Component | The constraint that forces it |
|---|---|
| **Kafka** | The SSE connection is **killed every 15 minutes by design**. Without a durable buffer you lose events on every reconnect. The classifier is also far slower than the feed — that gap is backpressure, the textbook case. And **replay is the experiment**: every configuration must see identical events, which offsets give you and a live socket cannot. |
| **Kubernetes** | Edit volume swings several-fold between US daytime and overnight, so **HPA on consumer lag is a real trigger**, not a staged load test. The ingester must survive a forced disconnect every 15 minutes. The nightly re-eval is a CronJob. Classifier and ingester scale independently. |
| **The model ladder** | Economically mandatory, not a study. At ~20 edits/second a frontier model is hundreds of dollars a day. The routing work is what makes the system exist at all. |
| **TypeScript / Node** | The services. Closes the gap that `CLAUDE.md` has carried for weeks, and earns back the `Node.js` line removed from the résumé on 2026-10-02. |

---

## What is verified, and what is not

**Verified** (`wikitech.wikimedia.org`, 2026-10-02):

- `stream.wikimedia.org`, the **`recentchange`** stream, **Server-Sent Events** over plain HTTP
- **No API key, no auth, no payment card.** Free.
- **Replay** via `since` or `Last-Event-ID`, with **7–31 days** of history
- **Connections are terminated at 15 minutes** by WMF's HTTP layer; clients must auto-resume
- Wikimedia runs it on Kafka internally and offers it to external tool developers

**Not verified — Phase 0 settles both before a line of system code is written:**

1. **The actual event rate.** If it is 2/second, Kafka is not justified and the honest move is to
   say so and stop. Measure it.
2. **Whether revert tags are reliably present** in the stream (`mw-reverted`, `mw-rollback`,
   `mw-undo`, `mw-manual-revert`). The free-label mechanism depends on it. Two documentation
   fetches did not confirm it; go and look.

---

## Honest limits, written before any results

- **A revert is not the same thing as vandalism.** Edits get reverted over content disputes, style
  disagreements and good-faith errors. The label is noisy **in both directions** — some vandalism
  survives unreverted, some good edits get reverted. Quantify the noise instead of pretending it
  is absent: hand-label a sample of 100 and report how often "reverted" and "vandalism" disagree.
- **ORES / LiftWing already does this**, with more resources and better data. **Do not claim to
  beat it on accuracy.** It is the baseline. The contribution is the cost-and-routing analysis,
  which is a different question and one nobody publishes.
- **Delayed labels.** You classify now; the truth arrives minutes to hours later. Anything
  unreverted after the window is *assumed* good, which is survivorship bias. State the window.
- **English Wikipedia only**, at least to start. Do not generalise across languages.
- **Local cluster, not production.** See the wording rules in Phase 8.

---

## Phase 0 — measure and predict before building anything

Nothing is built until this is done. It is the same move as predicting the speedup before renting
the A100 on Fusion Bench.

- connect to the stream for **one hour** and count: events/second, by wiki, by anonymous vs
  registered vs bot, and the diurnal pattern if you can sample twice
- confirm revert tags are present and queryable
- hand-label **100 edits** and measure how often "later reverted" agrees with "actually vandalism"
- **git-tag predictions before any run:** the vandalism base rate, the cheap model's accuracy, how
  much the expensive model adds, what fraction of traffic the heuristics can safely drop, and how
  many of these you expect to be wrong

**Done when:** the numbers exist and the Kafka decision is justified by one of them. **If the rate
does not justify Kafka, write that down and change the project.** That finding is worth more than
a system built on a false premise.

## Phase 1 — the ingester, in TypeScript and Node

- SSE client that resumes with `Last-Event-ID` after the 15-minute cut
- writes **raw events only** to Kafka — no parsing, no model, no opinions
- strict TS, Zod at the boundary, Vitest from the first commit

**Done when:** a forced disconnect loses nothing and duplicates nothing, proven by a test that
kills the connection mid-stream and diffs the output.

## Phase 2 — Kafka, and proving it earns its place

- one raw topic, one scored topic, one dead-letter topic; consumer groups; explicit offsets
- **replay from an arbitrary offset reproduces byte-identical output**
- **backpressure demo:** run a consumer deliberately slower than the feed, show lag growing and
  nothing lost — this is the artifact that justifies the whole component

**Done when:** the replay test passes and the lag graph exists.

## Phase 3 — the free filter, before any AI

Most edits are bots or long-established users and are obviously fine. Cheap, explainable rules:
is the user a bot, are they autoconfirmed, how large is the diff, does it touch references.

- measure **what fraction of traffic survives the filter**
- measure **what fraction of vandalism the filter throws away** — the recall you are paying for
  the cost saving

**Done when:** you can state the trade: "the filter removes N% of volume and loses M% of
vandalism."

## Phase 4 — labels, for free, from reverts

- join each edit to its later revert status within a stated window
- store `(edit, prediction, label, latency, config)` as the result table
- the sample from Phase 0 tells you how noisy this label is; carry that number everywhere

**Done when:** labels arrive automatically and the noise estimate is published beside them.

## Phase 5 — the grid

Three prompts x three models — **local Ollama**, **Gemini free tier**, **Groq free tier** — scored
against revert labels on a **sealed replay set** of captured edits.

- split: develop prompts on one offset range, score **once** on a sealed range
- all three prompts written and tagged **before** any of them runs
- **no judge model.** The label is the revert. An LLM grading an LLM is circular.
- baselines in the table: the Phase 3 heuristics alone, and ORES/LiftWing if reachable

**Done when:** the table exists, with baselines as rows, and you know which Phase 0 predictions
were wrong.

## Phase 6 — does the model know when it is wrong?

- bucket predictions by stated confidence; plot accuracy per bucket
- report **expected calibration error**, not only accuracy
- check whether calibration differs for anonymous vs registered editors

**Done when:** you can say yes or no on whether confidence is usable as a routing signal, with the
curve to prove it. A "no" is a real result and Phase 7 then has to route on something else.

## Phase 7 — the ladder: the economic core

heuristics → local model → cloud model, escalating only on uncertainty. Thresholds tuned on the
development range only.

Compare four policies on the sealed range: **heuristics only · local only · cloud only · ladder**.
Report for each: recall, precision, **dollars per 1,000 edits at dated list prices**, and p95
latency.

**Note on the free tiers:** the cloud providers' rate limits behave exactly like a cost ceiling,
which is a free and faithful simulation of the real constraint. But **report money from published
list prices, dated** — writing "$0 because free tier" would destroy the finding.

**Done when:** you can state the trade honestly, e.g. *"the ladder held 92% recall at 5% of
cloud-only cost"* — or that it did not beat local-only, which is equally worth publishing.

## Phase 8 — Kubernetes, driven by the real load

- Deployments for ingester and classifier, scaled independently
- **HPA on consumer lag**, demonstrated against the genuine diurnal swing rather than a synthetic
  load test
- liveness and readiness probes that actually fail when the service is sick
- `kind` in GitHub Actions: CI provisions a cluster, applies manifests, waits for readiness, runs
  smoke tests, tears it down — on every push

### Say it honestly

- **True:** *"Ingester and classifier on Kubernetes, autoscaled on Kafka consumer lag, with a CI
  job that provisions a throwaway cluster and verifies the deployment on every push."*
- **Never write:** production · managed cluster · live traffic at scale · served users.

## Phase 9 — drift

- nightly CronJob re-runs the **sealed** range against every configuration
- results stored as a time series; **a regression trips an alert**
- providers change models silently behind the same name; this is the system noticing

**Done when:** a deliberately degraded config is caught by the alert without you looking.

## Phase 10 — ship it and write the postmortem

- a public dashboard or feed someone other than you actually watches
- README carries: the Phase 5 table, the Phase 6 calibration curve, the Phase 7 policy comparison,
  the label-noise estimate, and the dated price table
- a write-up naming **where the model failed, how you noticed, and what you changed**

---

## Stack

**Target cost: $0, and no payment card anywhere.**

| Layer | Choice | Cost |
|---|---|---|
| Source | **Wikimedia EventStreams** `recentchange` (SSE, no auth) | **$0** |
| Language / runtime | **TypeScript** strict, **Node 22** | free |
| Transport | **Redpanda** (Kafka API) in-cluster | **$0** |
| Orchestration | **Kubernetes** — `k3d` locally, `kind` in GitHub Actions | **$0, no card** |
| Results store | **Neon** or **Supabase** Postgres free tier | **$0** |
| Cheap model | **local Ollama** — no rate limit, proven on the MCP/ChromaDB work | **$0** |
| Expensive model | **Gemini** and **Groq** free tiers — rate limits act as the cost ceiling | **$0** |
| Scheduling | Kubernetes **CronJob** | free |
| CI | Vitest + GitHub Actions | free |

**Why this is affordable at all:** the stream is captured into Kafka with **zero model calls**, and
every experiment runs offline by replaying stored offsets. Model spend is a bounded function of
how many experiments you run, not of the live edit rate.

**Laptop warning:** k3d + Redpanda + Postgres + two Node services + Ollama is real load. Keep the
local model small and do not run it continuously; you do not need to.

## Measurement discipline

- Nothing is reported that is not reproducible from a committed result file.
- Every cost figure carries the date of the price table it came from.
- The sealed offset range is scored **once per configuration**, never iterated on.
- Phase 0's predictions are quoted in the README beside what happened, right or wrong.
- The label-noise estimate from Phase 0 appears next to every accuracy number.

## Deliverables that are not code — these do more work than the code does

Most of what converts a project into an interview is not the repository. Build these deliberately.

| Artifact | Why it earns its keep |
|---|---|
| **A live dashboard** on Vercel: the feed arriving, each edit's ladder decision, the running cost counter | *"Here it is running right now"* beats every description. ShopBack's req asks for "a repository, demo, video, or write-up" — this is the strongest of the four and the cheapest to host. |
| **The write-up** — the cost curve, the calibration plot, the honest negatives | Most readers never open the code. This is the thing that circulates and gets quoted. |
| **A 2-minute video** of the live feed being classified | Cheap, memorable, and the one artifact a busy recruiter actually finishes. |
| **`DECISIONS.md`** — every architectural choice with the measured number that forced it | **This is the single highest-value document in the repo.** It turns *"why Kafka?"* from a trap into the best answer in the interview: "the connection dies every 15 minutes, the classifier runs at N/sec against a feed at M/sec, and every experiment has to replay identical events." Judgment, shown. |
| **`POSTMORTEM.md`** — where the model failed, how you noticed, what you changed | ShopBack asks for this in almost exactly those words. Write it as you go, not at the end. |
| **An architecture diagram** | One image. Goes in the README, the write-up and the interview. |
| **`docker compose up`** path alongside the Kubernetes one | Anyone can run it in 60 seconds without a cluster. Reproducibility is a quality signal in itself. |

**Lead the README with the finding, not the architecture.** Almost every project README opens with
"a system for X built with Y" and loses the reader. Open with the number: *"A frontier model costs
$380/day to run on Wikipedia's edit feed. A three-tier ladder held 92% of its recall for $19."*
Then explain how.

**Give it one quotable sentence.** One number, one comparison. That is what an interviewer repeats
to the next person in the loop, and it is what you lead with when someone asks "tell me about a
project."

## Fastest path to something usable on a résumé

The phases are ordered by dependency, not by urgency, and **he is applying now**. A measured result
in three weeks is worth more than a perfect system in three months.

**Minimum path: Phases 0 → 1 → 3 → 4 → 5.** That is the ingester, the free filter, revert labels,
and the grid — which produces the table, which is the bullet. Kafka can be a simple durable queue
at first; the formal replay/backpressure proof is Phase 2 and can follow.

**Then, in order of marginal value:** Phase 7 (the ladder — the strongest single result), Phase 6
(calibration), Phase 8 (Kubernetes), Phase 9 (drift).

**Do not do Phase 8 first** because Kubernetes is on the job description. Infrastructure with no
result attached reads as a tutorial, and it is the part of this project an intern is least likely
to be asked to own.

## What goes on a résumé, and when

**Nothing until Phase 5 produces the table.** After Phase 7 there are three bullets — the ladder's
cost result, the calibration finding, and the filter trade-off — and after Phase 9 a fourth about
catching silent model drift. All measured, all in his register, none of them claiming production.
