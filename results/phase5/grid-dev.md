# Phase 5 grid — dev set

200 edits that pass the filter; 39 reverted within 72h (base rate 19.5% [14.6%, 25.5%]).
**Target is revert, not vandalism:** in the Phase 0 sample 78.3% of reverted edits were not vandalism (claude-opus-5-5 (AI model, blind to revert status; not hand-labelled)).
The filter in front of every row already drops 16–24% of reverted edits (D10).

| Configuration | Precision | Recall | F1 | Flagged | Invalid | Latency p50 / p95 | Tokens in / out | Complete |
|---|---|---|---|---|---|---|---|---|
| baseline:filter-only | 19.5% [14.6%, 25.5%] | 100.0% [91.0%, 100.0%] | 0.326 | 100.0% | 0.0% | — | — | yes |
| baseline:filter+temporary | 23.3% [15.6%, 33.2%] | 51.3% [36.2%, 66.1%] | 0.320 | 43.0% | 0.0% | — | — | yes |
| baseline:liftwing-enwiki-damaging | 45.2% [29.2%, 62.2%] | 35.9% [22.7%, 51.6%] | 0.400 | 15.5% | 1.5% | 523 / 1524 ms | — | yes |
| baseline:liftwing-revertrisk-language-agnostic | 24.1% [17.9%, 31.7%] | 89.7% [76.4%, 95.9%] | 0.380 | 72.5% | 2.0% | 343 / 630 ms | — | yes |
