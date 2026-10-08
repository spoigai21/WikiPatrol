# Postmortem — written as it happened

Where things went wrong, how it was noticed, and what changed. Every number here comes from a
committed result file; the decision behind each fix is in `DECISIONS.md` (Dn). This document is
updated as the project runs, not at the end: the model results it is waiting on are marked as such.

---

## 1. The label was not measuring what the project said it measured

**What went wrong.** The plan was to detect vandalism, using "reverted within 72 hours" as a free
label. The Phase 0 label-noise sample found that **78% of reverted edits were not vandalism**
(95% CI 64–88%) — good-faith edits caught in someone else's revert, runs of one editor's link edits
undone together, typo fixes, even an anti-vandalism revert that was itself reverted. Against the
revert label, a perfect vandalism detector would have scored about 22% precision.

**How it was noticed.** The noise sample was in the plan from the start precisely because the label
was suspect; the size of the gap was the surprise (the prediction was under 20%).

**What changed.** The target was renamed to what the label is — *reverted within 72 hours* — and
every prompt, metric and sentence now says revert prediction, never vandalism detection (D9). ORES's
`damaging` model learns from the same kind of signal, so it remained a fair baseline.

**Caveat that still stands.** That sample was labelled by an AI model (blind to revert status), not
by hand as the spec requires; a hand re-label of the 50 reverted rows would replace it (D9).

## 2. Five Phase 0 predictions were wrong, and one premise was weaker than assumed

Predictions were git-tagged before any result was read (`phase0-predictions`), so they could be
wrong in public:

| Prediction | Measured |
|---|---|
| Revert rate 10–25% | **6.2%** [5.4, 7.2] |
| Reverted-but-not-vandalism under 20% | **78%** [64, 88] |
| Heuristics drop 30–60% of traffic | **73–87%** |
| The cloud model adds 10–25 points of recall | **17–36 points fewer** — it wins on precision instead |
| Stated confidence works as a routing signal | **Not for the local model** the ladder routes on (yes for the cloud ones) |

The Kubernetes argument assumed edit volume "swings several-fold" between day and night. Three
measured hours put the swing at **about 1.5×** (D8) — English Wikipedia is edited from every time
zone. The autoscaling case now rests on what was measured instead: a rate-limited model tier falls
behind even the quietest hour (D13), so lag is a real signal at any hour.

Kafka was likewise not justified by volume (1.1 classifiable edits/s, under the spec's own 2/s line,
D1). It was kept — and said to be kept — for backpressure, replay and independent consumers.

## 3. The free tiers were not what their documentation suggested

The project runs at $0, so free-tier limits are the cost ceiling. Each one was discovered by hitting
it, read from the error, and written into D12:

| Provider / model | What happened |
|---|---|
| Groq Llama models | No longer offered; `gpt-oss-120b` used instead |
| Gemini `gemini-2.5-flash` | Closed to new users |
| Gemini `gemini-3.8-flash` | Free tier: **20 requests a day** — the grid needs 3,000. Replaced by `gemini-3.5-flash-lite` before it ran on the sealed set (owner's decision) |
| Gemini `gemini-3.5-flash-lite` | Free tier: **500 requests a day** → about six days for its share of the grid |
| Groq `gpt-oss-120b` | 1,000 requests a day, but also **200,000 tokens a day** → about 330 requests a day, about eight days |
| — | The owner funded the Gemini runs on the paid tier: 5,671 calls, **$3.13 at list price**, done in under an hour; 3.8 Flash restored as the expensive tier; Groq left unfinished on its free tier (D12) |

**What changed.** The runner resumes where it stopped, recognises a daily cap (wherever the provider
buries it in the error) and stops cleanly instead of retrying, and a loop resumes it every 30
minutes. Cost is still reported from dated list prices (`results/prices/2026-10-04.json`), never as
$0.

## 4. The local model is overconfident and over-flags

**Where it failed** (sealed set, 1,000 edits that pass the filter, 16.7% of them reverted):
`gemma3:4b` flags 59–75% of edits and its precision (19–21%) is barely above the base rate. When it
says 90% likely to be reverted, about 20% are. Calibration error 0.40–0.54. Its scores still rank
reverted edits above kept ones better than chance (AUROC 0.60–0.63), which is what matters if it is
used as a router rather than a judge.

**How it was noticed.** The dev check flagged 14 of 20 on its first run; the prompts were already
frozen, so this was recorded as a result, not fixed. The reliability charts in
`results/phase6/sealed/` show the shape.

**Then the cloud models came in.** Gemini 3.8 Flash was the better judge (precision 31%, F1 0.39)
and its confidence was far closer to calibrated (error 0.16–0.20, AUROC 0.72). The local model's was
not, and that is what sank the ladder (section 4b).

## 4b. The ladder's rule assumed the wrong thing about which model finds more

**What went wrong.** The ladder's routing rule was fixed before the final scoring, as it should be:
choose the cheapest escalation that keeps 95% of the cloud model's recall. It assumed the expensive
model is the one that *finds* more reverts. It is not — the local model flags most of everything, so
its recall was higher (72.5% against 53.9%). The rule's target was already met by the local model
alone, so it escalated 2.6% of edits, and the "ladder" was the local model. The sentence the code
printed — "held 132% of cloud-only recall at 2.4% of its cost" — was arithmetically true and said
nothing.

**How it was noticed.** A ladder keeping *more* than 100% of the expensive model's recall is a
contradiction in the question itself; reading the policy table instead of the summary line made it
plain.

**What changed.** Nothing was re-tuned on the sealed set: the pre-registered result is the result
(the ladder did not beat local-only; D17). What it teaches is in the README's first lines: a cheap
model can only route if its confidence means something, and this one's did not (D18). A different
rule — say, sending up the edits the local model flags, for the cloud model to confirm — would be a
new configuration, developed on dev and scored once, and reported as post-hoc.

**The other misjudgement worth naming:** asking for "95% of cloud recall" treated recall as the
thing to protect. For revert prediction, where 78% of reverts are not vandalism, the cloud model's
advantage was precision — being right when it flags. The objective should have been chosen with
that in mind; it is now on the record why it was not.

## 4c. The autoscaling result was first measured against the wrong load

The Phase 8 recorder measured "the feed" as every edit reaching the classifier, and the plan said
replicas should follow it. Over 11.9 hours they did not (r = −0.03). The replicas only spend time on
the edits the free filter keeps, and overnight the feed grew with edits the filter drops. Against the
kept edits, read back from the broker afterwards, r = 0.87. That second measure was chosen after
seeing the first, and D16 says so beside both numbers. The lesson is the same as the label's: name
what is actually being measured — here, the model's workload, not edit volume.

## 5. Engineering failures

Each of these was caught, fixed, and given a test that fails without the fix.

| What broke | How it was noticed | What changed |
|---|---|---|
| 106 of 1,000 sealed diffs frozen as "unavailable" — the Action API was rate-limiting, not saying the content was gone | 11% unavailable on sealed against 1.5% on dev | Transient failures retried; only the API's own word counts; the 106 rows re-fetched before any run, sample unchanged (D12) |
| The laptop slept; runs stopped on "fetch failed" | Runs far behind schedule; timestamps had 1-hour gaps | Network failures retried (D12) |
| Groq's JSON mode rejected a malformed answer (`"p_revert":0. nine`) with a 400, and the run stopped | Run stopped at 50/1000 | Recorded as the model's invalid answer, as the rules always said (D12) |
| Two runners appended to one result file; one edit answered twice | Duplicate check after restarting lanes | One duplicate removed (both answers agreed); single-writer lock |
| `npm run ingest` and `npm run labeller` crashed on start since a Phase 1 fix — a CommonJS named import the test runner accepted and Node did not | First real start of the pipeline | Fixed; a test now loads every Kafka module the way Node runs it (D13) |
| Offsets were never committed (a no-op call), so every restart re-read from zero | Consumer groups missing from the broker | Explicit commits; a test asserts the committed offset (D13) |
| A routine consumer-group rebalance stopped a stage, and the whole pipeline with it | Backpressure graph went flat mid-run | Rebalances ridden out; stages supervised and restarted (D13) |
| A batch of outputs exceeded the broker's 1 MB request limit; the stage failed on it every restart | Second backpressure graph went flat; restart loop in the log | Every producer sends in ordered chunks under 512 KB (D13) |
| Two stages starting in the same millisecond shared a temporary consumer group; one waited forever | Two stages never logged their start | Unique group per call; a test reproduces the hang (D13) |
| The ingester could not start in a container (it created a local folder it did not need) | `docker compose` smoke test | Folder created only for the file sink |
| Classifier replicas replaced during a recovery stayed in the consumer group for 5 minutes (a session length chosen for slow stages), so rebalances never finished, liveness probes restarted the live replicas, and the group never settled | 18 group members for 6 pods, backlog not moving | The classifier uses a 45-second session; it heartbeats after every model call (D16) |
| An autoscaled classifier replica, evicted from its group while a slow model call ran, hit "the coordinator is not aware of this member" on its next commit; the uncaught error stopped its consumer for good, until the liveness probe restarted it | A pod restart 18 minutes into a fresh 24-hour window | A failed commit or heartbeat ends the batch and the replica rejoins; a test provokes the eviction and fails on the old code with the same error (D16) |
| After the cluster was stopped and started, the stages' group waited out dead members' 5-minute sessions; the liveness probe gave up at 5 minutes, and each restart left another dead member, so the group never re-formed | No parsing for 20 minutes; raw lag climbing past 800,000; group stuck rebalancing | Cleared by scaling the stages to 0 and back; a stage's liveness now waits out the group's worst-case settle time (D16) |
| The laptop slept on battery overnight, mid-run | A 5.4-hour hole in the Phase 8 record; `pmset` log | Run restarted; the rule is now "plugged in", because `caffeinate` cannot stop a low-battery sleep (D16) |
| KEDA could not resolve the broker from its own namespace | `ScaledObject` not ready on the local cluster | Fully qualified broker address everywhere (D14) |

Two backpressure runs were spoiled by these bugs; they are kept in `results/phase2/superseded/` and
not reported. The reported run (`results/phase2/backpressure-2026-10-04T0534Z.svg`) is the third.

## 6. What this changed about how the work is done

- **Freeze before measuring.** Predictions, prompts, filter rules, the drift threshold rule and the
  sealed set were each fixed and written down before the numbers they would be judged by existed.
  When something had to change after — the filter's description, the Gemini model — the change is
  dated and explained, and nothing already measured was rerun to look better.
- **A green test suite is not a running system.** Half of the failures above passed every test and
  broke only on real data, real Node, a real broker or a sleeping laptop. Each now has a test that
  reproduces it, and CI starts the whole stack on a real cluster on every push.
- **Validate against an independent source.** The stream labeller was checked edit by edit against
  the Action API (11,490 of 11,491 agree, D11) before any label was trusted.
