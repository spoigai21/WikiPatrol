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

## Phase 0 prediction scorecard

`PREDICTIONS.md` is frozen at tag `phase0-predictions` (7a4bd72). Scored here, by its own rule:
wrong = measured value outside the stated range.

| # | Predicted | Measured | Verdict |
|---|---|---|---|
| 1 | Vandalism base rate 5–15% | 1.4% point estimate (6.2% × 22% + 93.8% × 0%); the CI's upper end reaches ~9% | **Wrong on the point estimate**; not settled given the kept-side CI |
| 2 | Revert rate 10–25% | 6.2% [5.4, 7.2] | **Wrong** |
| 3 | P(not vandalism \| reverted) < 20% | 78% [64, 88] (AI-labelled, D9) | **Wrong** |
| 4–11 | — | Phases 3–7 | Not yet measurable |

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
