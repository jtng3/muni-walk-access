# Timing Spike — 2026-04-13

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
| network_build | 23.6 | 0.39 |
| address_fetch | 2.0 | 0.03 |
| gtfs_fetch | 75.9 | 1.26 |
| routing | 3.0 | 0.05 |
| **Total** | **104.8** | **1.75** |

## Memory

- Peak Python memory (tracemalloc): 836.8 MB
- Note: tracemalloc measures Python allocations only; C extensions (pandana, numpy) allocate outside Python's heap.

## Budget Projection

- Total time: 1.75 min
- Gate threshold: 20 min (within 30-min GHA budget)
- **Verdict: PASS: 1.7 min — within gate threshold (< 20 min)**
