# Phase 5 grid — sealed set

1000 edits that pass the filter; 167 reverted within 72h (base rate 16.7% [14.5%, 19.1%]).
**Target is revert, not vandalism:** in the Phase 0 sample 78.3% of reverted edits were not vandalism (claude-opus-5-5 (AI model, blind to revert status; not hand-labelled)).
The filter in front of every row already drops 16–24% of reverted edits (D10).

| Configuration | Precision | Recall | F1 | Flagged | Invalid | Latency p50 / p95 | Tokens in / out | Complete |
|---|---|---|---|---|---|---|---|---|
| baseline:filter-only | 16.7% [14.5%, 19.1%] | 100.0% [97.8%, 100.0%] | 0.286 | 100.0% | 0.0% | — | — | yes |
| baseline:filter+temporary | 24.2% [20.5%, 28.4%] | 66.5% [59.0%, 73.2%] | 0.355 | 45.8% | 0.0% | — | — | yes |
| baseline:liftwing-enwiki-damaging | 40.5% [32.2%, 49.4%] | 29.3% [23.0%, 36.6%] | 0.340 | 12.1% | 0.4% | 524 / 1915 ms | — | yes |
| baseline:liftwing-revertrisk-language-agnostic | 22.2% [19.2%, 25.5%] | 88.6% [82.9%, 92.6%] | 0.355 | 66.6% | 1.3% | 346 / 904 ms | — | yes |
| groq:openai/gpt-oss-120b__p1-plain | | | | | | | | running: 3/1000 |
| ollama:gemma3:4b__p1-plain | 19.4% [16.8%, 22.4%] | 86.8% [80.9%, 91.1%] | 0.318 | 74.6% | 0.0% | 1006 / 2083 ms | 479 / 18 | yes |
| ollama:gemma3:4b__p2-guide | 21.4% [18.2%, 24.9%] | 74.9% [67.8%, 80.8%] | 0.332 | 58.5% | 0.0% | 1082 / 2500 ms | 652 / 18 | yes |
| ollama:gemma3:4b__p3-reason | | | | | | | | running: 52/1000 |
