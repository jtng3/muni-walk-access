# Timing Spike — 2026-04-15

## Machine Info

- Platform: arm64
- OS: Darwin
- Python: 3.12.12

## Run Mode

- Mode: full
- Addresses: 232,505
- Stops: 3,172
- Routing results: 232,505

## Stage Timing

| Stage | Time (s) | Time (min) |
|---|---|---|
| network_build | 26.9 | 0.45 |
| address_fetch | 1.6 | 0.03 |
| gtfs_fetch | 0.5 | 0.01 |
| routing | 3.1 | 0.05 |
| stratify_lens | 1.2 | 0.02 |
| stratify_grid | 0.6 | 0.01 |
| stratify_hex | 10.3 | 0.17 |
| emit | 0.5 | 0.01 |
| **Total** | **46.6** | **0.78** |

## Memory

- Peak Python memory (tracemalloc): 836.8 MB
- Note: tracemalloc measures Python allocations only; C extensions (pandana, numpy) allocate outside Python's heap.

## Budget Projection

- Total time: 0.78 min
- Gate threshold: 20 min (within 30-min GHA budget)
- **Verdict: PASS: 0.8 min — within gate threshold (< 20 min)**
