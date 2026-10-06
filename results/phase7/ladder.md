# Phase 7 — the ladder (sealed set, scored once)

Rules: DECISIONS.md D17. Prices: list prices retrieved 2026-10-04. Revert prediction, not vandalism: 78.3% of reverted edits were not vandalism (claude-opus-5-5 (AI model, blind to revert status; not hand-labelled)).

## Cloud step: gemini:gemini-3.8-flash (p2-guide); local: ollama:gemma3:4b (p3-reason); escalate when p_revert in [0.6, 0.7)

| Policy | Precision | Recall | F1 | Sent to cloud | $ per 1,000 edits | p95 latency |
|---|---|---|---|---|---|---|
| heuristics only | 16.7% [14.5%, 19.1%] | 100.0% [97.8%, 100.0%] | 0.286 | 0.0% | $0.0000 | 0 ms |
| local only | 20.0% [17.0%, 23.4%] | 72.5% [65.2%, 78.7%] | 0.314 | 0.0% | $0.0000 | 4675 ms |
| cloud only | 30.6% [25.6%, 36.1%] | 53.9% [46.3%, 61.3%] | 0.391 | 100.0% | $0.2235 | 2421 ms |
| ladder [0.6, 0.7) | 20.1% [17.0%, 23.5%] | 71.3% [64.0%, 77.6%] | 0.313 | 2.6% | $0.0054 | 4877 ms |

**The ladder held 132.2% of cloud-only recall at 2.4% of cloud-only cost.**

## Cloud step: gemini:gemini-3.5-flash-lite (p2-guide); local: ollama:gemma3:4b (p3-reason); escalate when p_revert in [0, 0.05)

| Policy | Precision | Recall | F1 | Sent to cloud | $ per 1,000 edits | p95 latency |
|---|---|---|---|---|---|---|
| heuristics only | 16.7% [14.5%, 19.1%] | 100.0% [97.8%, 100.0%] | 0.286 | 0.0% | $0.0000 | 0 ms |
| local only | 20.0% [17.0%, 23.4%] | 72.5% [65.2%, 78.7%] | 0.314 | 0.0% | $0.0000 | 4675 ms |
| cloud only | 30.0% [25.0%, 35.5%] | 51.5% [44.0%, 59.0%] | 0.379 | 100.0% | $0.0802 | 1081 ms |
| ladder [0, 0.05) | 20.0% [17.0%, 23.4%] | 72.5% [65.2%, 78.7%] | 0.314 | 0.0% | $0.0000 | 4675 ms |

**The ladder held 140.7% of cloud-only recall at 0.0% of cloud-only cost.**
