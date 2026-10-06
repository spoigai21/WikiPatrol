# Phase 5 grid — sealed set

1000 edits that pass the filter; 167 reverted within 72h (base rate 16.7% [14.5%, 19.1%]).
**Target is revert, not vandalism:** in the Phase 0 sample 78.3% of reverted edits were not vandalism (claude-opus-5-5 (AI model, blind to revert status; not hand-labelled)).
The filter in front of every row already drops 13–24% of reverted edits (D10).

| Configuration | Precision | Recall | F1 | Flagged | Invalid | Latency p50 / p95 | Tokens in / out | Complete |
|---|---|---|---|---|---|---|---|---|
| baseline:filter-only | 16.7% [14.5%, 19.1%] | 100.0% [97.8%, 100.0%] | 0.286 | 100.0% | 0.0% | — | — | yes |
| baseline:filter+temporary | 24.2% [20.5%, 28.4%] | 66.5% [59.0%, 73.2%] | 0.355 | 45.8% | 0.0% | — | — | yes |
| baseline:liftwing-enwiki-damaging | 40.5% [32.2%, 49.4%] | 29.3% [23.0%, 36.6%] | 0.340 | 12.1% | 0.4% | 524 / 1915 ms | — | yes |
| baseline:liftwing-revertrisk-language-agnostic | 22.2% [19.2%, 25.5%] | 88.6% [82.9%, 92.6%] | 0.355 | 66.6% | 1.3% | 346 / 904 ms | — | yes |
| gemini:gemini-3.5-flash-lite__p1-plain | 27.6% [22.9%, 32.8%] | 50.9% [43.4%, 58.4%] | 0.358 | 30.8% | 0.0% | 574 / 755 ms | 464 / 19 | yes |
| gemini:gemini-3.5-flash-lite__p2-guide | 30.0% [25.0%, 35.5%] | 51.5% [44.0%, 59.0%] | 0.379 | 28.7% | 0.0% | 623 / 1081 ms | 637 / 21 | yes |
| gemini:gemini-3.5-flash-lite__p3-reason | 27.7% [23.2%, 32.8%] | 55.1% [47.5%, 62.4%] | 0.369 | 33.2% | 0.1% | 916 / 2028 ms | 667 / 69 | yes |
| gemini:gemini-3.8-flash__p1-plain | 28.2% [23.7%, 33.1%] | 59.9% [52.3%, 67.0%] | 0.383 | 35.5% | 0.0% | 1049 / 2159 ms | 464 / 33 | yes |
| gemini:gemini-3.8-flash__p2-guide | 30.6% [25.6%, 36.1%] | 53.9% [46.3%, 61.3%] | 0.391 | 29.4% | 0.0% | 1083 / 2421 ms | 637 / 53 | yes |
| gemini:gemini-3.8-flash__p3-reason | 31.0% [26.0%, 36.6%] | 53.9% [46.3%, 61.3%] | 0.394 | 29.0% | 0.0% | 1285 / 2741 ms | 667 / 103 | yes |
| groq:openai/gpt-oss-120b__p1-plain | | | | | | | | running: 979/1000 |
| groq:openai/gpt-oss-120b__p2-guide | | | | | | | | running: 58/1000 |
| groq:openai/gpt-oss-120b__p3-reason | | | | | | | | running: 8/1000 |
| ollama:gemma3:4b__p1-plain | 19.4% [16.8%, 22.4%] | 86.8% [80.9%, 91.1%] | 0.318 | 74.6% | 0.0% | 1006 / 2083 ms | 479 / 18 | yes |
| ollama:gemma3:4b__p2-guide | 21.4% [18.2%, 24.9%] | 74.9% [67.8%, 80.8%] | 0.332 | 58.5% | 0.0% | 1082 / 2500 ms | 652 / 18 | yes |
| ollama:gemma3:4b__p3-reason | 20.0% [17.0%, 23.4%] | 72.5% [65.2%, 78.7%] | 0.314 | 60.5% | 0.0% | 3068 / 4675 ms | 682 / 94 | yes |
