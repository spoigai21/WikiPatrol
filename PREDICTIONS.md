# Phase 0 predictions

Written before any model runs and before the label-noise and revert-rate results are read.
Tagged `phase0-predictions` once filled in. Never edited after the tag — the README quotes these
beside what actually happened, right or wrong.

Scope for every number: English Wikipedia, main namespace, edits and page creations, non-bot.

| # | Prediction | Value | Confidence (low/med/high) | Reasoning, one line |
|---|---|---|---|---|
| 1 | Vandalism base rate (share of classifiable edits that are vandalism) | 5–15% | med | Picked from rough brackets; no data consulted |
| 2 | Revert rate within the label window (share later tagged `mw-reverted`) | 10–25% | med | Picked from rough brackets; should be ≥ #1 |
| 3 | Label noise: P(not vandalism \| reverted) | < 20% | med | Most reverted edits are vandalism; the rest are mostly genuine mistakes made unknowingly |
| 4 | Label noise: P(vandalism \| not reverted) | no prediction | — |  |
| 5 | Share of traffic the Phase 3 heuristics can drop | 30–60% | med | Picked from rough brackets |
| 6 | Share of vandalism lost by that filter | < 10% | med | Vandalism from an established, verified account is unlikely |
| 7 | Cheap (local) model recall / precision against revert labels | no prediction | — |  |
| 8 | Recall the expensive (cloud) model adds over the cheap one, in points | 10–25 pts | low | Picked from rough brackets |
| 9 | Does stated confidence work as a routing signal (Phase 6)? yes / no | yes | med |  |
| 10 | Ladder: share of cloud-only recall kept, at what share of cloud-only cost | no prediction | — |  |
| 11 | How many of predictions 1–10 will turn out wrong (outside the stated range) | no prediction | — |  |

"Wrong" is defined now, so it cannot be redefined later: a prediction is wrong if the measured
value falls outside the range written here. Give ranges, not points. "No prediction" rows are
not scored either way.

Recorded 2026-10-02, before the revert-tag probe or any revert-rate result was read. The only
data seen beforehand was a 30-second rate smoke test (~1.5 classifiable enwiki edits/s), which
bears on none of the rows above.
