# Timing Spike — 2026-04-15

## Machine Info

- Platform: arm64
- OS: Darwin
- Python: 3.12.12

## Run Mode

- Mode: full
- Addresses: 232,505
- Stops: 3,181
- Routing results: 232,505

## Stage Timing

| Stage | Time (s) | Time (min) |
|---|---|---|
| network_build | 25.3 | 0.42 |
| address_fetch | 2.0 | 0.03 |
| gtfs_fetch | 75.7 | 1.26 |
| routing | 3.0 | 0.05 |
| stratify_lens | 1.1 | 0.02 |
| stratify_grid | 0.6 | 0.01 |
| stratify_hex | 1.0 | 0.02 |
| emit | 0.3 | 0.01 |
| **Total** | **109.6** | **1.83** |

## Memory

- Peak Python memory (tracemalloc): 836.8 MB
- Note: tracemalloc measures Python allocations only; C extensions (pandana, numpy) allocate outside Python's heap.

## Budget Projection

- Total time: 1.83 min
- Gate threshold: 20 min (within 30-min GHA budget)
- **Verdict: PASS: 1.8 min — within gate threshold (< 20 min)**
