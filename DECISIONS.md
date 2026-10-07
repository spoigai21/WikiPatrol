# Decisions

Every architectural choice, with the measured number that forced it. Raw measurements live in
`results/phase0/`; each entry names the file it came from.

---

## D1 — Kafka stays, but not because of volume (DECIDED 2026-10-02)

**Measured** (`results/phase0/rate-us-daytime-2026-10-02T1936Z.json`, one US-daytime hour,
2026-10-02 19:36–20:36 UTC, by event time):

| Scope | Mean/s | p95/s | p99/s | Max/s |
|---|---|---|---|---|
| All wikis, all event types | 38.5 | 64 | 79 | 328 |
| enwiki, all event types | 4.1 | 11 | 19 | 41 |
| enwiki, article edits + new pages (incl. bots) | 1.45 | 4 | 5 | 8 |
| enwiki, article edits + new pages, **non-bot** | ~1.14 (4,093/h) | — | — | — |

Per minute, enwiki article edits ranged 58–162 inside that single hour. The other two hours
(D8) put the non-bot classifiable rate at 0.83–1.31/s, so every sample is below the 2/s line.

**What this means.** The spec set "2/second" as the line below which Kafka is not justified on
volume. English Wikipedia's classifiable stream is **below that line**: ~1.1 non-bot edits/s,
about 98k a day. Volume alone does not justify Kafka for this scope.

**What still holds without the volume argument:**
- *Backpressure:* a classifier tier calling a rate-limited cloud model is far slower than
  1.1/s, so a buffer between ingest and classification is still needed — but a file or a table
  is also a buffer.
- *Replay:* needed for the experiments — but EventStreams itself replays 7–31 days via
  `Last-Event-ID`, and an append-only log with line offsets gives deterministic replay too.
- *Consumer lag as an autoscaling signal (Phase 8)* and *independent consumer groups* (live
  classifier, nightly re-eval, dashboard) are what Kafka specifically provides.

**Options considered:** drop Kafka for an append-only log and rework Phase 8; widen scope to all
wikis so volume forces it (rejected: storing data nothing classifies is decoration).

**Decision:** keep Kafka (Redpanda). It is justified by consumer lag as the autoscaling signal,
by backpressure from rate-limited model tiers, and by independent consumer groups — **not** by
volume, and every write-up says so. At ~1.1 edits/s a file would carry the load.

## D2 — Labels come from `mediawiki.revision-tags-change`, not `recentchange` (DECIDED)

**Measured** (`results/phase0/tags-probe-2026-10-02T1936Z.json`, 45 min): 0 of 103,845
`recentchange` events carry a tags field. `revision-tags-change` reported `mw-reverted` being
added 517 times on enwiki (236 in article space) in the same 45 minutes.

**Decision:** the ingester captures both streams; labels are joined on `rev_id` =
`recentchange.revision.new`.

## D3 — Reconnects are routine, not a 15-minute event (DECIDED)

**Measured** (same rate run): 5 server-side disconnects in one hour, at intervals of roughly
17, 6, 4, 2 and 15 minutes — not a clean 15-minute cut. Zero duplicates and zero gaps after
reconnecting with `Last-Event-ID`.

**Decision:** reconnect-and-continue is the ingester's normal path, covered by
`test/ingest.test.ts` (random cut points, mid-event cuts, replay overlap, process restart).

## D4 — Label window: reverted within 72 hours (DECIDED, provisional)

**Measured** (`results/phase0/tags-probe-2026-10-02T2022Z.json`, 45 min, 290 article-space
`mw-reverted` additions). Under a steady edit rate, the ages of revisions being reverted right
now follow the time-to-revert distribution:

| Reverted within | 1h | 6h | 24h | 72h | 7d | 30d |
|---|---|---|---|---|---|---|
| Share of all article reverts | 37% | 50% | 69% | 72% | 74% | 75% |

Median 8.2 hours. A quarter of article reverts undo edits more than 30 days old — some years
old — which is historical cleanup, not patrol. Of reverts within 30 days, 24h catches 92% and
**72h catches 96%**.

**Decision:** label = `mw-reverted` within 72 hours. Edits reverted later count as not
reverted, which adds to label noise in the "kept but vandalism" direction; the hand-labelled
sample measures that. Because the Action API reports current tags, not when they were added,
the Phase 0 sample queries edits aged 72–96h.

**Why provisional:** the first probe (`tags-probe-2026-10-02T1936Z.json`, all namespaces) gave
a median of ~2.8 days, so 45-minute windows are noisy. Re-check from a day or more of
`revision-tags-change` events captured by the ingester; change the window only if the 72h
coverage moves materially, and before any Phase 5 scoring.

## D5 — One partition for the raw topic (DECIDED)

At ~1.1 classifiable edits/s (D1), and even at the 38/s all-wiki rate, one partition has ample
headroom. One partition gives a total order, so "offsets 1,000,000–1,100,000" names exactly one
set of events — which is what the sealed replay range needs. Classifier parallelism (Phase 8)
comes from the scored side, not from splitting the raw log.

## D6 — `kafkajs` as the Kafka client (DECIDED, revisit)

`@confluentinc/kafka-javascript` (native librdkafka, maintained) has no prebuilt binary for
Node 26 and failed to build. `kafkajs` is pure JS and works against Redpanda v26.2.3, but it has
had no release since 2023 and emits a `TimeoutNegativeWarning` on Node 26. Contained behind the
`RawSink` interface (`src/ingest/sink.ts`), so swapping it later touches one file.

## D7 — Exactly-once into the raw topic by dedupe, not transactions (DECIDED)

The source is at-least-once (reconnecting replays a few events). The producer is idempotent, every
message carries its own SSE id as a header, and the checkpoint is the last message in the
topic, so a partially written batch can only cause a replay. Replays are dropped by `meta.id`
against the tail of the topic. Verified: `test/kafka-sink.test.ts` (random disconnects plus a
process restart → 500 of 500, in order, byte-identical; with restart dedupe disabled it fails
with 503), and live: 4,932 events across a restart, 0 duplicate keys.

## D8 — The diurnal swing is ~1.5x, not several-fold (MEASURED; Phase 8 consequence open)

**Measured** — three one-hour samples, counted by event time. The two marked *replay* were read
from the stream's history with `--since` (`npm run phase0:rate -- --since …`); counting by event
time makes a replayed hour measure the same thing as a live one.

| Hour (UTC) | US local | Source | All wikis/s | enwiki classifiable/s (incl. bots) | **non-bot**/s | Per-minute, non-edge |
|---|---|---|---|---|---|---|
| Wed 2026-09-30 12:00 | 08:00 EDT | replay | 44.1 | 1.32 | **1.31** | 47–155 |
| Fri 2026-10-02 09:00 | 02:00 PDT | replay | 33.1 | 0.89 | **0.83** | 36–79 |
| Fri 2026-10-02 19:36 | 12:36 PDT | live | 38.5 | 1.45 | **1.14** | 58–162 |

Files: `results/phase0/rate-us-morning-2026-09-30T1200Z.json`,
`rate-us-overnight-2026-10-02T0900Z.json`, `rate-us-daytime-2026-10-02T1936Z.json`.

**What this means.** US overnight to US daytime is about **1.4–1.6x** in hourly mean for the
classifiable stream — English Wikipedia is edited from every timezone, and 09:00 UTC is daytime in
Europe and South Asia. Minute-level bursts (36 → 162 per minute) swing more than the hourly mean.
The SPEC's Phase 8 premise, that volume "swings several-fold" so HPA on lag is a real trigger, is
**weaker than assumed**. Three hours is not a full day; the trough may sit elsewhere.

**Not decided here** — it changes what Phase 8 demonstrates, so it is the owner's call. Options:
(a) keep HPA on lag but argue it from tier capacity, not volume — a rate-limited cloud tier
saturates below 0.83/s, so lag grows at any hour; (b) capture a full 24h before Phase 8 and report
the measured swing, whatever it is; (c) narrow Phase 8 to "autoscaling works on lag", without the
diurnal claim.

## D9 — The target is "reverted within 72h", because most reverts are not vandalism (DECIDED 2026-10-03)

**Measured** (`results/phase0/label-noise.json`, sheet `labels/sample.csv`, key
`results/phase0/sample-key.json`): 100 non-bot enwiki article edits from 2026-09-30 12:00–13:00
UTC, stratified 50 reverted / 50 kept, revert status read at 72–96h.

| | Rate | 95% CI | n (excl. unsure) |
|---|---|---|---|
| P(not vandalism \| reverted) | **78%** | 64–88% | 46 |
| P(vandalism \| not reverted) | **0%** | 0–7% | 49 |
| Unsure | 5 rows | | |

Reweighted to the 6.2% population revert rate, revert status agrees with "vandalism" on 95% of
edits — but only because almost everything is neither. Of the reverted edits, 10 were vandalism.
The other 36 were mostly good-faith edits caught in someone else's revert: runs of one editor's
link edits reverted together (5 on one article), typo fixes, an anti-vandalism revert that was
itself reverted. A plausible mechanism, **not verified**: a multi-revision undo tags every
revision in the range `mw-reverted`, including good edits sandwiched between bad ones.

**How the labels were made — read before quoting these numbers.** The sheet was labelled by an
AI model (Claude Opus 5.5), not by hand, at the owner's direction. It saw each diff through the
Action API `compare` endpoint, which returns no tags, and did not open the key until scoring was
done. Definition used: **vandalism = deliberate damage** (nonsense, insults, blanking, sneaky
falsification); good-faith errors, puffery and content disputes are "not vandalism". A broader
"damaging" definition would move some reverted rows to positive and lower the 78%. Every
non-obvious call has a one-line reason in the sheet's `notes` column. A hand re-label of the 50
reverted rows would replace this estimate; the spec's "hand-labelled" requirement is **not** met
until then.

**What this means.** Scored against revert labels, a classifier that finds exactly the vandalism
would look like it has ~22% precision. Most "false positives" against the revert label would be
good edits that happened to be reverted. Phase 5 as written ("the label is the revert") measures
**revert prediction**, not vandalism detection.

**Decision:** rename the target to what the label is: **"reverted within 72 hours"**. Every
prompt, metric and write-up says revert prediction — revert recall, revert precision — never
vandalism detection. ORES's `damaging` model is trained on the same kind of signal, so it stays a
fair baseline. The 78% figure is quoted beside the results as the gap between the two questions.

**Not chosen:** cleaning the label (only single-revision reverts, rollbacks or anti-vandalism
tools), and a hand-labelled gold set for the headline number. Either can be added later as a
separate, additional measurement; neither changes the target chosen here.

## D10 — The free filter uses Wikipedia's own trust levels, fixed before scoring (DECIDED 2026-10-03)

Written before any filter result was computed.

**Rules** (`src/filter/rules.ts`), three policies, from least to most aggressive:

| Policy | Drops |
|---|---|
| `bots` | edits flagged bot |
| `extendedconfirmed` (**default**) | + registered accounts at least 30 days old with at least 500 edits |
| `autoconfirmed` | + registered accounts at least 4 days old with at least 10 edits (a looser bar, so it drops more) |

*(Corrected after scoring: the first draft listed `autoconfirmed` as the middle tier. The rules
did not change; only this ordering was wrong.)*

The thresholds are the ones Wikipedia itself uses to grant trust, so there is nothing to tune and
nothing to overfit. Temporary accounts are never dropped. The default is the conservative policy,
chosen before scoring; the other two are reported beside it as the trade-off curve.

**Inputs only from the event plus a stored user snapshot**, so the filter is deterministic on
replay (Phase 2). Account age is measured at the time of the edit. The edit count is read when the
snapshot is taken — a few days after the edit for the evaluation tables — so an account that
crossed 10 or 500 edits in between is counted as trusted early. This can only make the filter
look slightly more aggressive than it was.

**Not used, though the spec lists them:** diff size and "touches references". Neither is in the
`recentchange` event; both need the diff text, one Action API call per edit. That is cheap but not
free, and it is a feature for the models, not a reason to drop an edit unseen.

**Evaluation sets** — tables in `results/phase3/` (`npm run phase3:table`), one row per
classifiable enwiki edit with its revert label, no usernames:
- *dev:* 2026-09-30 12:00–13:00 UTC, the hour the Phase 0 sample came from;
- *held-out:* 2026-09-30 04:00–05:00 UTC, replayed for this and not examined before scoring.

Both are reported. Nothing is tuned on either, so "held-out" is a second sample, not a guard.
These are Phase 0 replays, not the Kafka log; the M% is re-measured on Phase 4 labels from the
log. **Reported:** N = share of edits removed (all, and non-bot); M = share of later-reverted
edits removed, with a Wilson 95% interval.

### D10 results — the filter removes 70–87% of volume and loses 16–24% of later-reverted edits

`results/phase3/filter-eval.json`, from `results/phase3/edits-*.csv` (labels read 78–87h after
the edits).

| Table | Policy | Volume removed (all / non-bot) | Reverted edits lost (95% CI) | Revert rate before → after |
|---|---|---|---|---|
| dev 09-30 12:00 | `bots` | 1.2% / 0% | 0% [0, 1.4] | 5.9% → 5.9% |
| | **`extendedconfirmed`** | **73.3% / 72.9%** | **16.3% [12.4, 21.1]** | 5.9% → 18.4% |
| | `autoconfirmed` | 84.0% / 83.8% | 36.5% [31.0, 42.3] | 5.9% → 23.2% |
| held-out 09-30 04:00 | `bots` | 57.2% / 0% | 0% [0, 2.6] | 2.2% → 5.1% |
| | **`extendedconfirmed`** | **87.1% / 69.8%** | **24.0% [17.8, 31.5]** | 2.2% → 12.7% |
| | `autoconfirmed` | 91.3% / 79.6% | 29.4% [22.7, 37.3] | 2.2% → 17.5% |

**The trade, in the spec's shape:** the default filter removes **73–87% of edits** (70–73% of
non-bot edits) and loses **16–24% of later-reverted edits**. The bot share alone swings from 1% to
57% between the two hours (D8), which is why the non-bot column is the stable one.

**What the lost reverts are.** In the Phase 0 sample (dev hour), the default filter drops 12 of
the 50 reverted edits, and all 12 were labelled *not vandalism*; all 10 vandalism edits pass the
filter. So the reverts it gives up look like the good-faith reverts D9 found — consistent with
trusted accounts' edits being reverted over disputes, not damage. The sample is small and
AI-labelled (D9); this is an indication, not a measurement.

**Status:** provisional, as the spec says — M is re-measured on Phase 4 labels from the Kafka log.

**Re-measured on Phase 4 labels (2026-10-06)** — the stream labeller (D11) over the replayed tag
stream, instead of the Action API; three hours instead of two, adding the sealed hour (the filter's
rules are fixed, so nothing is tuned on it). `results/phase3/filter-eval-stream-labels.json`:

| Hour (2026-09-30 UTC) | Edits removed | Non-bot removed | Reverted edits lost |
|---|---|---|---|
| 12:00 (dev) | 73.3% | 72.9% | 16.2% [12.3, 21.0] |
| 04:00 (held-out) | 87.1% | 69.8% | 24.1% [17.9, 31.7] |
| 10:00 (sealed hour) | 67.0% | 66.0% | 12.6% [8.8, 17.5] |

The two hours measured before agree with the Action API version to within one edit, as D11's
validation said they would. **The filter removes 67–87% of edits (66–73% of non-bot edits) and
loses 13–24% of later-reverted edits.** Phase 3 is done on Phase 4 labels; the same measurement on
labels from the live cluster follows once its log is 72 hours old (`npm run phase4:live`).

## D11 — Labels are computed from the raw log, in event time (DECIDED 2026-10-03)

**What a label is** (`src/labels/labeller.ts`): one per classifiable enwiki edit (main namespace,
edit or page creation, bots included — the filter decides what to drop, not the labeller):

| Label | Meaning |
|---|---|
| `reverted` | `mw-reverted` was *added* to the revision within 72h of the edit (D4) |
| `deleted` | not reverted, but the page was deleted within the window |
| `incomplete` | not reverted, but the raw log has a hole inside the window, so "not reverted" is unproven |
| `not-reverted` | none of the above |

Rates use `reverted / (reverted + not-reverted)`; `deleted` and `incomplete` are counted, never
silently folded into either side.

**Why from the log and not the Action API.** The Action API says what a revision's tags are *now*,
not when they were added, so it can only approximate a 72h window by asking at the right moment
(the Phase 0 sample asked at 72–96h). The log has the time of every tag change, so the window is
exact, and the labels can be recomputed from the log forever — the API's answer drifts.

**Deterministic by construction.** A window closes when the log's *event time* passes edit + 72h
+ 10 min of grace for out-of-order events — never on the wall clock. Labels are emitted in source-
offset order, and the labeller's checkpoint is the source offset on the last label written to
`wiki.labels`, the same pattern as the ingester (D7). Replaying the same offsets, or stopping and
restarting, gives the same labels: `test/kafka-labeller.test.ts`, which fails if resume is broken.

**Outages.** A jump of more than 2 minutes in event time (the all-wiki feed never idles that long,
D8) is a hole in the log; every edit whose window overlaps it is `incomplete`. The ingester's
`Last-Event-ID` resume closes holes shorter than the stream's 7-day replay window, so in practice a
hole means the ingester was down for over a week.

**The result table** `(edit, prediction, label, latency, config)` needs predictions, which start
in Phase 5. Phase 4 provides its label column, keyed by `rev_id` in `wiki.labels`; the table
itself is built in Phase 5 (the Postgres store in the Stack is not created until then).

**Running it:** `npm run ingest` and `npm run labeller` (both against `docker compose up -d`). The
first labels appear 72 hours after the ingester first starts; `npm run phase4:summary` publishes
counts and the revert rate with the D9 noise estimate beside them.

### D11 results — the stream labeller agrees with the Action API on 11,490 of 11,491 edits

`results/phase4/validation-*.json` (`npm run phase4:validate`). Input: the Phase 3 hours' edits,
merged by event time with every enwiki `revision-tags-change` event from 2026-09-30 04:00 to
2026-10-03 13:20 UTC, replayed from EventStreams (601,224 events, all from the active datacenter's
topic, no gaps). Compared with the Action API labels in the Phase 3 tables, asked at 78–87h.

| Hour | Compared | Agree | Disagree |
|---|---|---|---|
| dev 09-30 12:00 | 4,736 (277 reverted) | **4,736** | 0 |
| held-out 09-30 04:00 | 6,755 (146 reverted per API) | **6,754** | 1 |

The one disagreement is not a labeller error: the edit (rev 1377608275, *W. C. Fields*) was
reverted at 2026-10-03 18:30, **86h** after the edit — outside the 72h window — and the API, asked
at 87h, already showed the tag. Checked by hand against the page history. So on these two hours
the stream labels and the API labels match wherever the 72h definition and the API's "as of now"
answer coincide.

**What this does and does not cover.** It validates the join and the window on real data. It does
not exercise `deleted` (the Phase 0 captures hold no deletion log events) or `incomplete` (the
replay had no holes); both are covered by unit tests only. The 20 edits the API reported deleted
or suppressed were labelled `not-reverted` (19) and `reverted` (1) by the stream, which cannot see
revision deletion; the live labeller would see page deletions in `recentchange`, but not
revision suppression.

**Phase 3 tables, re-read in this light:** their API labels over-count reverts slightly — any edit
reverted between 72h and the query is counted as reverted. On these two hours that was 1 edit.

## D12 — The Phase 5 grid: sets, prompts, models and scoring, fixed before any run (DECIDED 2026-10-03)

Written before either evaluation set was built and before any prompt was run.

**Evaluation sets** (`npm run phase5:set`, `results/phase5/sets/`). Replayed hours, not offset
ranges of `wiki.raw`: the log does not yet hold an hour whose 72h window has closed. What the
offset range was for — identical, permanent inputs for every configuration, now and in Phase 9 —
is carried by the committed snapshot and its SHA-256 instead.

| Set | Hour (UTC) | Edits | Status |
|---|---|---|---|
| dev | 2026-09-30 12:00–13:00 | seeded sample of 200 that pass the filter | examined (the Phase 0 sample came from it); used only to check plumbing |
| **sealed** | 2026-09-30 10:00–11:00 | seeded sample of **1,000** that pass the filter (all, if fewer) | never examined; scored **once** per configuration |

Seed 20261003. Labels from the Phase 4 labeller over the replayed tag stream (D11). Each edit's
diff is fetched once and frozen in the snapshot (3,000-character cap, marked when truncated), so a
later page edit or deletion cannot change what a model sees.

**Repair before any run (2026-10-03).** The first sealed build froze 106 diffs as "unavailable"
when the Action API was in fact rate-limiting (HTTP errors, while the dev baselines ran against
the same API). The fetcher now retries transient failures and only accepts the API's own word that
content is gone; `--repair` re-fetched exactly those 106 rows and kept every other byte, so the
sample did not change. Unavailable diffs: 4 of 1,000 (dev: 3 of 200). The manifest records the
repair and the previous checksum. Final sealed SHA-256: `82fe1aa9968ff3d5…` (full value in
`results/phase5/sets/sealed.manifest.json`). No configuration had run on the sealed set.

**Population.** The models see only edits that pass the default filter (D10), so the grid is
scored on that population. The filter's own loss (D10: 16–24% of reverted edits) is carried
beside it, and Phase 7 combines the two.

**Prompts** (`prompts/`): three, all asking *will this edit be reverted within 72 hours?* (D9),
all answering in the same JSON — `{"revert": true|false, "p_revert": 0..1}` (`p_revert` feeds
Phase 6).
- **p1-plain** — the task, the edit, the answer format.
- **p2-guide** — adds a short guide to what English Wikipedia patrollers typically revert and keep.
- **p3-reason** — p2, plus 1–3 sentences of reasoning before the answer.

Written and git-tagged `phase5-prompts` before any run. The runner refuses the sealed set unless
each prompt file is byte-identical to its tagged version. Dev runs check parsing and plumbing; if
one shows a prompt must change, the change gets a new tag, a dev rerun, and an entry here.

**Models** (one per tier, temperature 0, fixed seed where the API takes one):
- local — Ollama **`gemma3:4b`** (installed; small enough to run beside the stack on a laptop);
- cloud — **Gemini** and **Groq** free tiers: the exact model IDs are fixed here, before the first
  sealed run, once API keys exist. Every result row records the model ID the API reports.

**Cloud models, fixed 2026-10-03 before any sealed cloud run** (from the models each key could
use that day; the Llama models are no longer offered on Groq, `gemini-2.5-flash` is closed to new
users):
- **Groq `openai/gpt-oss-120b`** — the largest model offered; `reasoning_effort: low`.
  Free-tier limits on this account (response headers): 1,000 requests/day, 8,000 tokens/minute.
- **Gemini `gemini-3.8-flash`** — the current Flash model; `thinkingLevel: low`. *(Replaced by
  `gemini-3.5-flash-lite` before it ran on the sealed set — see below.)*

Both reason before answering, and reasoning counts as output, so these two get a 1,024-token
output cap instead of 400; "low" keeps the reasoning short and the free-tier budget intact. Both
are deliberate cost settings, reported with the results. The local model does not reason, so its
cap (400) never binds — its sealed answers average under 100 tokens.

Dev check (8 edits per prompt, prompts as tagged): no invalid answers from either; ~600 tokens
and ~0.5 s per call on Groq, ~450 tokens and 5–7 s on Gemini (with 503 "high demand" retries).
At these free-tier limits the six sealed cloud runs (6,000 calls) take several days.

**Gemini model replaced (2026-10-04, owner's decision).** `gemini-3.8-flash`'s free tier allows
**20 requests per day** (quota `GenerateRequestsPerDayPerProjectPerModel-FreeTier`, read from the
429), against the 3,000 the grid needs — about 150 days. It is replaced by
**`gemini-3.5-flash-lite`** (`thinkingLevel: low`; it does not reason in practice). The two sealed
answers `gemini-3.8-flash` gave are kept in `results/phase5/runs/abandoned/` and not scored. The
replacement had not been run on the sealed set; its dev check (8 edits per prompt) gave no invalid
answers, ~0.6 s per call. List price $0.30 / $2.50 per million tokens (`results/prices/2026-10-04.json`).
Its own free tier turned out to be **500 requests a day** (read from its 429 on 2026-10-04), so its
three sealed runs take about six days — paced, like Groq's, by the free quota.
The runner also failed to recognise this cap as a daily one (it is named only deep in the error
body) and kept retrying for 25 minutes; it now reads the whole body and the "retry in …h" hint.

**Cloud tiers become Gemini-only, on the paid tier (2026-10-06, owner's decision; written before any
paid call).** The free tiers were pacing the grid to about a week (Groq: 200,000 tokens a day;
Gemini Flash-Lite: 500 requests a day). The owner funded $5 of Gemini usage — which changes the
SPEC's "$0, no card" target for this one provider, and is reported as such. From now:

| Tier | Model | Status |
|---|---|---|
| cheap cloud | Gemini **3.5 Flash-Lite** | sealed runs finished on the paid tier; same model, prompts, settings |
| expensive cloud | Gemini **3.8 Flash** (`thinkingLevel: low`) | restored — the model first chosen in this section, dropped only for its 20-a-day free cap |
| (bonus) | Groq `gpt-oss-120b` | left running on its free tier in the background; reported if it completes, not waited on |

Gemini Pro was considered and not chosen: at $2.00 / $12.00 per million tokens, with reasoning that
cannot be switched off, three prompts would cost about $12. Estimated spend at list prices
(`results/prices/2026-10-04.json`): Flash-Lite ~$0.66 and 3.8 Flash ~$1.45, sealed and dev together.

**Disclosed:** `gemini-3.8-flash` answered 2 sealed edits on 2026-10-04 before it was set aside
(`results/phase5/runs/abandoned/`). Those answers were never scored or examined. Its sealed run now
starts from zero and answers those 2 edits again, once, as part of its single scoring. The prompts
(tag `phase5-prompts`), the sealed set and the scoring rules are unchanged.

**Two runner fixes during the sealed runs (2026-10-04), neither changing any answer already
recorded:** (1) a request that gets no response at all (the laptop slept and woke without network)
is now retried instead of stopping the run; (2) when Groq's JSON mode rejects a malformed answer
with a 400 (`json_validate_failed`, e.g. `"p_revert":0. nine`), the rejected text is now recorded as
the model's answer and scored `invalid`, as this section always specified, instead of stopping the
run. Rows are appended one complete line at a time, so the stopped runs resumed where they were.

Free-tier rate limits are honoured by a resumable runner (results are appended per edit and a
restart skips what is done); a sealed run may take days. That is the cost ceiling the spec
describes, not a reason to shrink the set.

**Output handling.** Each response is parsed against the JSON contract. Anything else is
recorded as `invalid`, counted in the table, and scored as "not flagged" — a model that cannot
answer does not get the benefit of the doubt.

**Baselines** (rows in the same table, same edits):
- *filter only* — the Phase 3 filter flags everything it passes (recall 100% by construction;
  precision = the base rate);
- *filter + temporary accounts* — flag only edits from temporary accounts;
- *LiftWing `revertrisk-language-agnostic`* — Wikimedia's revert-risk model, flag at p ≥ 0.5;
- *LiftWing `enwiki-damaging`* (ORES) — flag at p ≥ 0.5.
LiftWing is queried now for edits made days ago; whether its features are as-of-edit or as-of-now
is not verified, so it may have information the LLMs do not. Not claimed beaten either way (D9).

**Scoring** (`npm run phase5:score`): revert precision, recall and F1 with Wilson intervals, share
flagged, share invalid, p50/p95 latency, tokens per edit — and the D9 label-noise estimate beside
every number. **No judge model:** the label is the revert.

## D13 — The pipeline: one-in, one-out stages, and why Kafka earns its place (DECIDED 2026-10-04)

**Topics** (all one partition, kept forever — D5, `src/kafka/topics.ts`):

```
wiki.raw ─┬─ parse  ─> wiki.edits ── enrich ─> wiki.enriched ── filter ─> wiki.scored ─> (model tiers)
          ├─ dlq    ─> wiki.dlq
          └─ labeller ─> wiki.labels
```

The spec asks for a raw, a scored and a dead-letter topic; there are two more because of one fact:
the filter needs each editor's account age and edit count, which only a live Action API lookup
gives, and that lookup is not repeatable. So `enrich` is its own stage, the only non-deterministic
one, and it *records* what it saw (and when) in `wiki.enriched`. Everything else — `parse`, `dlq`,
`filter`, the labeller — is a pure function of its input topic, and replays byte-for-byte.

**Stage semantics** (`src/kafka/stage.ts`). Each stage reads one topic and writes one; every output
carries its input's offset. Live, a stage is a consumer group that commits its input offset only
after the outputs are durable; a crash in between means re-reading, and re-read outputs are
recognised by their source offset and not written again. Replay mode reads an exact offset range,
with no group. `npm run pipeline` runs every stage under a supervisor that restarts a failed stage
with backoff, so one stage failing never stops the others — the shape Phase 8's restart policy
takes over.

**Proved** (`test/kafka-replay.test.ts`, real Redpanda):
- replaying `parse`, `dlq` and `filter` over the same range into a fresh topic gives byte-identical
  output, and an arbitrary middle range gives exactly the matching slice of a full run;
- a live stage stopped mid-stream and restarted writes every output exactly once — including after
  its committed offset is wiped, the worst case of a crash before a commit — and fails if the
  source-offset check is removed;
- on real data: after the parse stage re-read 1.34 million events it had already processed,
  `wiki.edits` held 64,511 messages, 64,511 distinct edits, in source order.

**Backpressure** (`npm run phase2:backpressure`, `results/phase2/backpressure-2026-10-04T0534Z.{json,svg}`).
With the ingester and every stage caught up to the live feed (2026-10-04 05:34–05:55 UTC), a
consumer standing in for a free-tier cloud model — 6 s per edit the filter keeps, 10 a minute
(D12) — read `wiki.scored` while ~74 edits a minute arrived (1.2/s, D1). Lag rose steadily, about
25 messages a minute, to **514 after 20 minutes**; switched to full speed it drained to zero in
under 15 seconds, and every offset from 204,978 to 206,542 (1,565) was handled **exactly once** —
no gap, no duplicate. A model tier this slow cannot keep up with even the quietest English
Wikipedia hour without a buffer that holds what it has not reached yet, and the lag itself is the
number that says how far behind it is: Phase 8's autoscaling signal, measured here first.

**Dead letters.** Two sources: events the ingester cannot key (it sends them straight to
`wiki.dlq`), and events no stage can read (schema failures). 7 in the first 2.05 million raw events.

**Found while building it, all fixed with a test that fails without the fix:**
1. `kafkajs` is CommonJS; a named import (`ConfigResourceTypes`) that the test runner accepted
   crashed under Node — so `npm run ingest` and `npm run labeller` had been broken since the
   Phase 1 retention fix. `test/runtime-imports.test.ts` now loads every Kafka module the way Node
   runs it.
2. Offsets were never committed (`commitOffsetsIfNecessary` is a no-op without auto-commit); the
   source-offset check kept outputs exactly-once regardless, but every restart re-read from zero.
3. A routine consumer-group rebalance stopped a stage and, with it, the whole pipeline process —
   in the middle of the first backpressure run. Rebalances are now ridden out and stages are
   supervised.
4. A stage sent a whole batch's outputs in one request; a batch of enriched records exceeded the
   broker's 1 MB limit, and the filter stage failed on the same batch every restart — in the middle
   of the second backpressure run. Every producer (stages, labeller, ingester) now sends in ordered
   chunks under 512 KB.
5. On restart, two stages in one process sometimes hung forever: the helper that reads a topic's
   last message named its temporary consumer group by process id and millisecond, so two stages
   starting in the same millisecond, reading different topics, shared a group and one never got
   its partition. Groups are now unique per call.

Both interrupted backpressure runs flattened out where the stalled stage stopped feeding
`wiki.scored`; they are kept in `results/phase2/superseded/` and not reported.

## D14 — Kubernetes: one image, a partitioned scored topic, and a classifier that scales on lag (DECIDED 2026-10-04)

**Why `wiki.scored` is the one partitioned topic.** Autoscaling the model tier only helps if
several replicas can share its input, and a consumer group gives each partition to one member.
So `wiki.scored` has **6 partitions** (keyed by rev id) and the classifier scales **1 → 6** on
its consumer lag (KEDA `ScaledObject`, which drives an HPA; target 50 waiting edits per
replica). Every other topic stays at one partition, where the stages are exactly-once (D13). The
fan-out is **at-least-once**: after a crash or rebalance an edit can be classified twice;
`wiki.predictions` is keyed by rev id, and readers keep one. The filter stage's restart check for a
partitioned output takes the *smallest* last-written source offset over the partitions, so a
crash can rewrite a few outputs but never skip one (`src/kafka/stage.ts`); a stage refuses a
partitioned *input*.

**The classifier** (`src/classifier/`) passes filter-dropped edits through as "not flagged" and
sends kept edits to one tier: `heuristic` (flag temporary accounts — the default, needs no model,
used in CI), a model (`ollama:<model>:<prompt>`, or a cloud one), or `slow-heuristic:<ms>` (the
heuristic at a fixed pace, standing in for a rate-limited model in scaling demos). The full
ladder routing is wired in once Phase 7 is tuned.

**Probes that fail when the service is sick** (`src/ops/health.ts`): `/livez` fails when a
service has made no progress for 5 minutes *while work is waiting* — a stuck consumer, a dead
stream — not merely when the process is gone; `/readyz` once it has started. The ingester counts
as stuck after 5 quiet minutes (the all-wiki feed never idles that long, D8).

**Manifests** (`deploy/k8s/`): Redpanda (single node, dev mode — not a production broker), one
ingester (`Recreate`: two would write every event twice), the stages, the classifier and its
`ScaledObject`. Brokers are addressed by full cluster DNS name so KEDA's operator, in another
namespace, can reach them; an init container waits for Redpanda so pods never crash-loop on a cold
start. `docker compose up -d --build` runs the same services without a cluster.

**CI** (`.github/workflows/ci.yml`, `deploy/ci-smoke.sh`): every push runs the typecheck and all
tests against a real Redpanda, then provisions a `kind` cluster, installs KEDA, deploys, waits for
every rollout, checks that live Wikipedia edits reach `wiki.predictions`, that `wiki.scored` has 6
partitions, that the `ScaledObject` is reading lag, and that no container restarted — then tears
the cluster down. Validated end to end on a local `kind` cluster (2026-10-04) before any push.

**Observed** on that local cluster with the classifier slowed to 10 kept edits a minute: the HPA
scaled it from 1 to 6 replicas on lag within minutes and held 6 while the average lag sat at the
target. A smoke demonstration, not the Phase 8 result — that is the HPA following the real
diurnal swing over 24 hours, which needs the laptop up for a day.

## D15 — Drift: a fixed subset, nightly, against each configuration's own sealed answers (DECIDED 2026-10-04)

Written before any drift run.

**Why not the whole sealed set.** The spec's nightly rerun of the sealed range against every
configuration is ~9,000 calls a night; Groq's free tier allows ~330 a day (D12). So the nightly
check reruns a **fixed subset of 50 sealed edits** per configuration — chosen once by seeded
shuffle and committed (`results/phase9/subset.json`) — and compares each answer with the answer
the same configuration gave on the same edit in Phase 5. The inputs are byte-identical (the frozen
snapshot), so a change in answers is a change in the model, the provider, or the prompt plumbing.

**What trips the alert**, per configuration: agreement with its own Phase 5 decisions below the
threshold, a different model version reported by the API, or the invalid-answer rate up by more
than 5 points. The agreement threshold is set from **measured run-to-run variance**, not assumed:
before the first nightly run, the subset is run twice back to back per configuration, and the
threshold is the lower of 90% and that baseline agreement minus 5 points. Recorded here when
measured.

**These runs are monitoring, never tuning** (SPEC Phase 9): they are written to
`results/phase9/`, never appended to the Phase 5 run files, and a prompt or model change prompted
by drift is a new configuration with its own dev work and its own single sealed score.

**Alert channel.** A non-zero exit (a failed Kubernetes Job), an entry in
`results/phase9/alerts.jsonl`, and a POST to `DRIFT_WEBHOOK` if set (e.g. an ntfy.sh or Slack
URL). **Proof it works:** a deliberately degraded configuration (`degraded:<config>:<percent>`,
which flips that share of answers deterministically) must trip the alert with nobody looking.

**When.** Cloud configurations join the nightly check after their sealed runs finish, so drift
calls never compete with sealed calls for the free quota.

### D15 results — run-to-run variance is near zero, and a degraded config is caught

**Variance** (`results/phase9/thresholds.json`, 2026-10-04): the 50-edit subset rerun twice, back to
back, on the five configurations with a complete sealed run that can drift (the three local-model
prompts and both LiftWing models). Four matched their own Phase 5 decisions on all 50 edits in
both reruns; `gemma3:4b` with `p3-reason` matched on 49 of 50 — the same edit both times, so it is
stable now but one answer differs from the sealed run made the day before. Threshold: the lower of
90% and (98% − 5 points) = **90% agreement**.

**Gemini added (2026-10-06)** — the same two back-to-back reruns for the six Gemini configurations
(600 calls, $0.27 at list price). Flash-Lite matched its own sealed answers on all 50 edits with
every prompt. 3.8 Flash matched on 49, 48 and 48 of 50 (98%, 96%, 96%) — and its two reruns agreed with
each other completely, so the 1–2 answers that differ changed *between* the sealed run and a few hours
later, not from call to call. A small, real shift: exactly what this check exists to see, and inside
the threshold, which stays at **90%** (the lower of 90% and 96% − 5 points) across all 11 configurations.

**The alert works** (`results/phase9/drift-2026-10-04T0640Z-degraded.json`): `gemma3:4b`/`p1-plain`
with 30% of its decisions flipped scored 72% agreement (14 of 50 changed); the run exited 1 and
wrote `results/phase9/alerts.jsonl` with nobody watching. The CronJob that runs this nightly is
`deploy/k8s/optional/drift-cronjob.yaml`, for the laptop's k3d cluster (it mounts the repo's
`results/`); it has not yet run on a schedule.

## D16 — The Phase 8 run: the autoscaler against a real day, with a paced model tier (DECIDED 2026-10-04)

Written before the run started.

**What it shows.** The classifier's replica count following the real feed over at least 24 hours
— the spec's Phase 8 criterion — on a `kind` cluster on the laptop, fed live from Wikimedia.

**The classifier tier** is the local model (`ollama:gemma3:4b`, prompt `p2-guide`, the best local
configuration by F1 on the sealed set) behind the filter, as the spec asks: heuristics plus a small
local model, no cloud APIs.

**Why it is paced.** Unpaced, the local model answers in about a second, and the kept-edit stream
is about 15–21 a minute (D8, D10): one replica would never fall behind and the autoscaler would
have nothing to do all day. So each replica makes **at most one model call every 12 seconds**
(5 a minute), standing in for a per-replica model quota — one API key per replica, as a rate-limited
cloud tier would impose. The pace was chosen from the measured rates *before* the run, so that the
real feed needs about 3 replicas at night and 4–5 by day, inside the 1–6 range. The replica count
over the day is then the real feed's doing; the pace only sets the scale.

**Recorded** once a minute (`npm run phase8:record`, `results/phase8/diurnal-*.jsonl`): HPA
replicas and the lag metric it scales on, the classifier group's total lag, and the raw, scored and
predictions topics' sizes (whose minute-to-minute differences are the feed rates).

**Incidents during the run, logged as they happened:**
- 2026-10-04 18:39–20:38 UTC: a first recording window, ended when Claude Code stopped the recorder
  for low system memory; kept as `results/phase8/interrupted-diurnal-2026-10-04T1839Z.jsonl`, not
  reported. The idle `docker compose` Redpanda was stopped to free memory, and the 24-hour window
  restarted at 2026-10-05 00:14 UTC (`diurnal-2026-10-05T0014Z.jsonl`).
- 2026-10-05 ~04:47–04:51 UTC: Ollama was quit on the host, so the model tier answered nothing for
  about four minutes; the backlog grew and the HPA scaled from 3 to 6 replicas until it returned.
  The cluster itself did not restart anything.
- 2026-10-05 10:02–15:26 UTC: the Mac went into "Low Power Sleep" on battery (`pmset` log) and woke on
  AC power. 5.4 hours unrecorded — the quietest US night hours. On wake, liveness probes restarted
  the stages and classifier pods (no progress for hours: the probes doing their job), the ingester
  resumed from its checkpoint and replayed the missed hours, and the HPA went to 6 replicas on the
  catch-up backlog (~10,700) — a recovery burst, not the diurnal feed, and excluded as such. A second,
  clean recording window starts automatically once that backlog has drained, to capture 24
  continuous hours if the Mac stays on power.
- 2026-10-05 16:45 UTC: Claude Code stopped the recorder for low memory again; restarted at 19:02 on
  the owner's go-ahead. The post-sleep backlog had **not** drained (~24,000 messages: at 6 replicas ×
  5 calls a minute the paced tier barely outruns the live feed), so that recording
  (`interrupted-diurnal-2026-10-05T1902Z-backlog.jsonl`) is set aside too.
- 2026-10-05 19:02–19:21 UTC, recovery: the backlog was drained by switching the classifier to the
  instant `heuristic` tier for about a minute (those predictions are not used in any evaluation), then
  the paced model was restored. This exposed a bug: the classifier's 5-minute consumer session (set
  for the stages in Phase 2) kept replaced replicas in the group, every rebalance waited on them, the
  live replicas made no progress, liveness probes restarted them, and the group never settled (18
  members for 6 pods). The classifier now uses a 45-second session; the group settled within a
  minute. **The clean 25-hour window starts at 2026-10-05 19:21 UTC** (`diurnal-2026-10-05T1921Z.jsonl`) on normal traffic.
- 2026-10-05 21:10–22:12 UTC: Claude Code stopped the recorder for low memory once more; restarted
  on the owner's request, appending to the same file. These 62 minutes are unrecorded.
- 2026-10-05 ~21:42–21:57 UTC, inside that gap: while the host was critically short of memory, the
  stages and classifier pods stopped making progress (Kafka answering slowly) for over five minutes;
  their liveness probes failed with 503 and Kubernetes restarted them (stages twice, classifiers
  once or twice each — 10 restarts in all), after which they recovered on their own. The probes did
  what they are for; it still breaks this window's "no restarts" bar, and is reported as such.
- 2026-10-06 07:16 UTC: the recording stopped about 12 hours in, when the Mac restarted (uptime
  shows a boot at ~08:27 UTC). The cluster came back on its own at ~16:00 UTC; the ingester resumed
  from its checkpoint and replayed the missed hours, leaving the classifier ~27,800 behind. The
  19:21 window is kept as a ~12-hour partial record (with the 62-minute gap and the 21:42 restarts above).
- 2026-10-06 16:23–16:26 UTC, recovery, the same way as on 10-05 (owner's go-ahead): the classifier was
  switched to the instant `heuristic` tier until lag reached 0 (those predictions are not used in any
  evaluation), then the paced `gemma3:4b`/`p2-guide` tier (12 s pace) was restored; the group settled
  to 6 members within two minutes. A new window started at 16:31 UTC (`diurnal-2026-10-06T1631Z.jsonl`).
- 2026-10-06 16:44–16:49 UTC, a bug: a classifier replica was evicted from the group (a model call
  outlasted its session while six replicas shared one Ollama) and its next offset commit failed with
  "the coordinator is not aware of this member". The classifier did not catch commit failures, so
  kafkajs stopped that consumer for good; the replica sat idle until its liveness probe restarted it.
  Every scale event risks this, so the 16:31 window (40 minutes) is set aside. Fixed the way the
  stages already handle it: a failed commit or heartbeat ends the batch and the replica rejoins
  (`test/kafka-classifier.test.ts` provokes the eviction; it fails on the old code with the same error).
  The image was rebuilt and the classifier redeployed at 17:12 UTC. A window started at 17:16 UTC
  (`diurnal-2026-10-06T1716Z.jsonl`) was stopped after two minutes on the owner's request; the cluster
  kept running, unchanged. A further window started at 17:31 UTC (`diurnal-2026-10-06T1731Z.jsonl`)
  and was stopped at 17:48 UTC on the owner's request, with the cluster (`docker stop`) and Ollama.
- 2026-10-07 00:12 UTC, restart: Ollama and the cluster were started again, the image rebuilt and
  every deployment redeployed. The stages group then stuck rebalancing for ~20 minutes: the old pods'
  members held their 5-minute sessions, the stage made no progress, its liveness probe restarted it,
  and the restart left one more stale member — the classifier's D16 bug, in the stages. Cleared by
  scaling stages to 0 until the group emptied, then back to 1 (settled 00:35). The ~6.5 missed hours
  then reached the classifier (~33,000 behind), drained on the instant `heuristic` tier 00:43–00:45 as
  before, and the paced `gemma3:4b`/`p2-guide` tier was restored. Not fixed in code yet: the stages'
  5-minute session (Phase 2) is longer than a redeploy can wait. **The 25-hour window starts at
  2026-10-07 00:48 UTC** (`diurnal-2026-10-07T0048Z.jsonl`).

**Success** = over 24 hours, replicas rise when the feed rises and fall when it falls, with no
container restarts and no gap in the record longer than the laptop was asleep — reported as it
comes out, including if the swing is too small to move the replica count.

## D17 — The Phase 7 ladder: everything chosen on the dev set, then scored once (DECIDED 2026-10-06)

Written after the Gemini runs finished and **before any sealed result for them, or any sealed ladder
result, was computed or looked at.**

**The ladder:** filter → local model → cloud model, escalating only when the local model is unsure
(SPEC Phase 7). The cloud step is **Gemini 3.8 Flash**, the expensive model the project asks about.
A second ladder with **Gemini 3.5 Flash-Lite** as the cloud step is reported beside it, as the
cheap-cloud variant.

**Chosen on dev only, by fixed rules:**
- *Prompt for each model* — the one with the highest revert F1 on the dev set (200 edits); a tie
  goes to the prompt with fewer tokens per edit.
- *When to escalate* — the local model's `p_revert` band [lo, hi) with the lowest escalation rate
  whose dev recall is at least **95% of cloud-only dev recall**, ties to higher F1; an invalid local
  answer always escalates (`chooseBand`, `src/phase7/ladder.ts`).

**Scored once on the sealed set,** four policies on the same 1,000 edits that pass the filter:
heuristics only, local only, cloud only, ladder — recall, precision, F1, **dollars per 1,000
classifiable edits** at the dated list prices (`results/prices/2026-10-04.json`; the filter's
share of edits that reach a model is the sealed hour's own, 1,132 of 3,428), and p95 latency.
The filter's own loss (D10) sits in front of every policy and is stated beside the result.

**The headline takes the shape fixed in the SPEC:** "the ladder held Y% of cloud-only recall at Z%
of cloud-only cost" — or, if it does not beat local-only, that.

### D17 results — the pre-registered ladder did not beat local-only

`results/phase7/ladder.{json,md}`, scored once, 2026-10-06. Chosen on dev by the rules above: local
`gemma3:4b` with `p3-reason`; cloud `gemini-3.8-flash` with `p2-guide`; escalate when the local
`p_revert` is in [0.6, 0.7).

| Policy (1,000 sealed edits past the filter) | Precision | Recall | F1 | Sent to cloud | $ per 1,000 classifiable edits |
|---|---|---|---|---|---|
| heuristics only | 16.7% | 100% | 0.286 | 0% | $0 |
| local only | 20.0% | 72.5% | 0.314 | 0% | $0 |
| **cloud only (3.8 Flash)** | **30.6%** | 53.9% | **0.391** | 100% | **$0.22** |
| ladder | 20.1% | 71.3% | 0.313 | 2.6% | $0.005 |

**What happened.** The rule asked for the cheapest ladder that keeps 95% of cloud-only recall. It
assumed the expensive model is the one that *finds* more reverts. It is not: the local model flags
about 60% of everything and so has the higher recall (72.5% against 53.9%); the cloud model is the
one that is *right* more often (precision 30.6% against 20.0%, best F1 of any model). The recall
target was met almost without escalating, so the ladder is the local model with a sliver of cloud
on top — "132% of cloud-only recall at 2.4% of its cost", true and useless. In the SPEC's own
words, the honest result is that **the ladder did not beat local-only** (F1 0.313 against 0.314),
and that the expensive model beats both on F1. The same happened with Flash-Lite as the cloud step
(escalation 0%).

**Why — Phase 6 answers it.** The ladder routes on the *local* model's confidence, and that
confidence is close to useless: calibration error 0.40–0.54, AUROC 0.60–0.63. The cloud models'
confidence is far better (ECE 0.16–0.20, AUROC ~0.72, level with Wikimedia's own LiftWing models at
0.72–0.74). So: **confidence is usable as a routing signal for the Gemini models, and not for the
small local model** — and a ladder whose first step cannot tell when it is unsure cannot route.

**Not done, deliberately:** a different routing rule chosen after seeing these sealed numbers. That
would be tuning on the sealed set (D12). A new rule is a new configuration: developed on dev, then
scored once — and reported as a second, post-hoc result, never in place of this one.

**Cost, at the 2026-10-04 list prices.** Cloud-only on 3.8 Flash costs **$0.22 per 1,000
classifiable English Wikipedia edits** — after the filter has removed two-thirds of them for free;
without the filter it would be about $0.68. At the 3,200–5,200 classifiable edits an hour measured
in Phase 0 (D8), that is roughly **$17–28 a day**. The runs themselves: 5,671 Gemini calls on the paid
tier, **$3.13 at list price**.

## D18 — Phase 6 verdict: confidence routes for the cloud models, not the local one (MEASURED 2026-10-06)

`results/phase6/sealed/calibration.json` and reliability charts. Expected calibration error / AUROC
on the sealed set:

| Model | ECE | AUROC |
|---|---|---|
| `gemma3:4b` (local) | 0.40–0.54 | 0.60–0.63 |
| Gemini 3.5 Flash-Lite | 0.18–0.20 | 0.69–0.70 |
| Gemini 3.8 Flash | 0.16–0.20 | 0.72 |
| LiftWing revert-risk / damaging | 0.46 / 0.09 | 0.74 / 0.72 |

**Yes** for the cloud models: their stated probability ranks reverted edits above kept ones about as
well as Wikimedia's models, and is far closer to calibrated. **No** for the local model: when it says
90%, about 20% are reverted. Temporary vs registered editors are broken out in the JSON.

## Phase 0 prediction scorecard

`PREDICTIONS.md` is frozen at tag `phase0-predictions` (7a4bd72). Scored here, by its own rule:
wrong = measured value outside the stated range.

| # | Predicted | Measured | Verdict |
|---|---|---|---|
| 1 | Vandalism base rate 5–15% | 1.4% point estimate (6.2% × 22% + 93.8% × 0%); the CI's upper end reaches ~9% | **Wrong on the point estimate**; not settled given the kept-side CI |
| 2 | Revert rate 10–25% | 6.2% [5.4, 7.2] | **Wrong** |
| 3 | P(not vandalism \| reverted) < 20% | 78% [64, 88] (AI-labelled, D9) | **Wrong** |
| 5 | Heuristics drop 30–60% of traffic | 73–87% (default policy, D10) | **Wrong** (above the range) |
| 6 | Filter loses < 10% of vandalism | 0 of 10 vandalism rows dropped (95% CI 0–28%); 16–24% of *reverted* edits lost | **Within range** on vandalism, CI too wide to settle; the revert-target figure (D9) is above it |
| 8 | Cloud model adds 10–25 points of recall over the local one | **−17 to −36 points**, same prompt against same prompt: the local model has *higher* recall (it over-flags); the cloud models win on precision | **Wrong** |
| 9 | Stated confidence works as a routing signal: yes | Yes for the cloud models, **no for the local model** the ladder routes on (D18) | **Wrong** for the routing that mattered |
| 4, 7, 10, 11 | no prediction | — | Not scored |

## Noted, not yet a decision

- enwiki has **no IP editors** in the samples: logged-out edits now arrive as temporary accounts
  (`~2025-…`). "Anonymous" in this project means temporary accounts: 9–15% of article edits
  across the three hours.
- Bot share of enwiki article edits **varies from 1% to 21%** between the three hours (D8), so
  "how much the filter drops" depends on when it is measured. The Phase 3 filter removes bots for
  free, but report its volume cut over a full day, not one hour.
- **Population revert rate: 6.2% [5.4%, 7.2%]** of non-bot enwiki article edits were tagged
  `mw-reverted` when queried at 72–96h old (2,989 edits, 2026-09-30 12:00–13:00 UTC;
  `results/phase0/sample-key.json`). A further 0.4% were deleted or suppressed and carry no tag.
- Replayed hours had 34–80 events that failed the `RecentChange` schema, against 2 live; not
  investigated, under 0.1% of events.
