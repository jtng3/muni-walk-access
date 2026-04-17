# Timing Spike — 2026-04-14

## Machine Info

- Platform: arm64
- OS: Darwin
- Python: 3.12.12

## Run Mode

- Mode: sample (n=100)
- Addresses: 232,505
- Stops: 3,181
- Routing results: 100

## Stage Timing

| Stage | Time (s) | Time (min) |
|---|---|---|
| network_build | 24.4 | 0.41 |
| address_fetch | 1.8 | 0.03 |
| gtfs_fetch | 75.9 | 1.26 |
| routing | 1.6 | 0.03 |
| stratify_lens | 0.2 | 0.00 |
| stratify_grid | 0.2 | 0.00 |
| emit | 0.1 | 0.00 |
| **Total** | **104.6** | **1.74** |

## Memory

- Peak Python memory (tracemalloc): 836.8 MB
- Note: tracemalloc measures Python allocations only; C extensions (pandana, numpy) allocate outside Python's heap.

## Budget Projection

- Total time: 1.74 min
- Gate threshold: 20 min (within 30-min GHA budget)
- **Verdict: PASS: 1.7 min — within gate threshold (< 20 min)**
