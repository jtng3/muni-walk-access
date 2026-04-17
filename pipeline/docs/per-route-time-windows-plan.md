# Per-Route Frequency & Time-of-Day Windows

**Status:** Reviewed — consensus incorporated
**Date:** 2026-04-15
**Reviewed:** cross-functional (product, UX, architecture, analysis) + engineering

---

## Problem

The current pipeline computes a single `trips_per_hour_peak` per stop, aggregated across all routes during 7-9 AM only. This hides critical equity information:

- A stop served by 4 routes at 3 trips/hr each shows 12 trips/hr aggregate, but a resident needing a *specific* route waits 20 minutes.
- Service deserts outside rush hours are invisible — midday, evening, and overnight service gaps disproportionately affect elderly, disabled, caregivers, and service workers.
- Route cuts/reductions can't be assessed per-neighborhood without per-route data.

## Design

### Primary Metric

**`best_route_headway_min`** (60 / max single-route frequency) is the default hex coloring — not aggregate trips/hr. This answers the policymaker's question: "How long does a resident wait for their bus?" Aggregate hides the inequity this tool exists to expose.

**`route_count`** surfaces separately as a resilience/redundancy indicator — a stop with 1 route at 10-min headway is far more fragile than 3 routes at 10-min. Do not conflate into the headway number.

### Data Model

**Per-route detail** (for stop popup drilldown):
```
stop_id | route_id | route_short_name | time_window | trips_per_hour | stop_lat | stop_lon
```

**Summary** (drives hex grid scoring and downstream pipeline):
```
stop_id | time_window | best_route_headway_min | total_trips_per_hour | route_count | stop_lat | stop_lon
```

Implementation: single DataFrame with a `level` column (`"route"` or `"summary"`). Summary rows flow downstream to lens.py/grid.py. Per-route detail is a separate artifact for the stop popup only.

### Time Windows

| Key | Label | Start | End | Policy relevance |
|-----|-------|-------|-----|------------------|
| `am_peak` | AM Rush | 06:00 | 09:00 | Commuters, school access |
| `midday` | Midday | 09:00 | 15:00 | Elderly, disabled, caregivers, part-time |
| `pm_peak` | PM Rush | 15:00 | 19:00 | Return commute, after-school |
| `evening` | Evening | 19:00 | 00:00 | Service workers, nightlife, safety |
| `overnight` | Overnight | 00:00 | 06:00 | Owl service — hospital, janitorial workers |

Configurable in `config.yaml` via a `time_windows` list in `FrequencyConfig`.

**GTFS >24h time handling:** Departures like "25:00:00" (1 AM next day) must bin into `overnight`, not treated as hour 25. The existing `_parse_time_seconds` handles >24h parsing; the binning logic must mod by 86400.

### Pipeline Changes (Phase 0 + Phase 1)

#### Phase 0: Calendar-aware GTFS filtering (prerequisite)

The current code counts ALL trips including Saturday/Sunday-only service, inflating weekday numbers. Before multi-window work:

1. Read `calendar.txt` and `calendar_dates.txt` from the GTFS zip.
2. Filter to weekday service IDs (Monday-Friday) by default.
3. Make weekday/weekend configurable for future weekend analysis.

#### Phase 1: Per-route, multi-window frequency + hex grid scoring

**These are a single unit of work** — not independently shippable because `lens.py` sits between GTFS output and grid scoring.

**`gtfs.py`:**
1. Read `routes.txt` (new file read — not currently parsed).
2. Join `trips.txt` → `stop_times.txt` on `trip_id` to get `route_id` per stop visit.
3. Join `routes.txt` to get `route_short_name` (e.g., "14", "N", "30").
4. Bin each departure into a time window (with >24h modular handling).
5. Group by `(stop_id, route_id, time_window)` → count distinct trips → compute `trips_per_hour`.
6. Derive summary per `(stop_id, time_window)`: `best_route_headway_min`, `total_trips_per_hour`, `route_count`.
7. Ship as `_compute_stop_frequencies_v2` — clean break, don't try backward compat with old function.

**`lens.py` (must update in same phase):**
- Current join on `stop_id` will cartesian-explode with multiple rows per stop. Fix: join only the summary table, filtered to a specific time window, or pivot to per-window columns.

**`grid.py`:**
- Don't refactor. Loop existing `compute_hex_grids` once per time window at the orchestration level (`__main__.py`). grid.py stays a pure single-window scorer.

**`frequency_bin` classification:** Apply per time window. A stop that is "high frequency" at AM peak and "low frequency" at 10 PM should show both. Same bin thresholds apply — the time window reveals the story.

**Output: Option B** — pre-compute hex scores per time window. One file per resolution per time window:
```
site/src/data/grid_hex_r9_am_peak.json
site/src/data/grid_hex_r9_midday.json
site/src/data/grid_hex_r9_pm_peak.json
site/src/data/grid_hex_r9_evening.json
site/src/data/grid_hex_r9_overnight.json
```

Same `HexGridSchema` with one added field: `time_window: str`. Minimal schema change. This also solves the payload size problem — frontend loads only the active window.

**Cache:** Parquet cache key already includes content hash; new schema produces different hash, no collision with old caches.

### UI Changes (Phase 2)

**Progressive disclosure** — the controls panel is already dense:

1. **Time window selector** (primary, visible by default, above sliders) — dropdown or segmented control.
2. **Route mode toggle** ("All routes combined" vs "Best single route") — tucked into Advanced/secondary panel. Policymakers care about "when" before "how the metric is calculated."
3. **Stop detail popup** — on hex/stop click, show **top 3 routes by frequency** for the selected time window, worst headway highlighted in red. "Show all" expander for full breakdown. Don't show a 12-row table upfront.
4. **Lazy-load per window** — fetch only the selected time window's hex file. Skeleton/shimmer state on switch. Don't ship 5x payload upfront.

**Label:** Use "Longest wait for your route" instead of "best_route_headway_min" — frame the metric around the experience, not the computation.

### Animation (Phase 3 — stretch/optional)

Marked as stretch. A static dropdown switching between windows gives 95% of the insight. Animation adds polish, not analytical value.

If built:
1. **Play button** — auto-cycles through windows. Prefetch next window while displaying current.
2. **Pause on click** anywhere on the map (presenters point at screens).
3. **Large, obvious current-window label** during playback.
4. **Skip overnight by default** in animation — opt-in. Map going dark at 2 AM creates a jarring moment that distracts from the midday/evening equity story.
5. Playback speed control (1s, 2s, 5s per window).

## Incremental Delivery

| Phase | Scope | Depends on |
|-------|-------|------------|
| 0 | Calendar-aware GTFS filtering (weekday service IDs) | GTFS URL fix (done) |
| 1 | Per-route multi-window frequency + lens.py fix + grid loop + per-window hex JSON | Phase 0 |
| 2a | Time window selector + route mode toggle + lazy loading | Phase 1 |
| 2b | Stop detail popup with per-route table (top 3 + expander) | Phase 2a |
| 3 | Time-of-day animation loop (stretch) | Phase 2a |

**Deploy constraint:** Pipeline and site must ship together when schema changes — the frontend will break if hex JSON schema changes without corresponding UI update.

## Resolved Questions

| # | Question | Decision | Rationale |
|---|----------|----------|-----------|
| 1 | Best 1 or 2 routes for headway? | **Single best route** | Metric answers "how long do I wait for my bus." Transfers/redundancy captured separately by `route_count`. |
| 2 | Overnight weighting? | **Same weight, no special treatment** | The whole point is making gaps visible. Map going dark IS the message. UI handles storytelling. |
| 3 | Composite all-day score? | **Not in pipeline** | Per-window scores preserve equity signal. Composite can be derived client-side later if needed. |
| 4 | `frequency_bin` per window? | **Yes, per window** | Same thresholds, applied independently. "High at AM peak, low at evening" is the equity story. |
| 5 | Payload size? | **Per-window files, lazy-load** | One file per resolution per window. Frontend fetches only active window. Animation prefetches next. |

## Open Items

- **Weekend service:** Not in scope for v1 but flagged as Phase 1.5. Weekend deserts are major equity gaps for service workers and families. GTFS `calendar.txt` has the data; Phase 0 calendar filtering lays the groundwork.
- **Direction of travel:** GTFS `direction_id` available — a stop may have 12 trips/hr inbound but 4 outbound. Deferred but cheap to add later.
- **Validate with Luke:** Confirm per-route breakdown is actually needed by SFMTA, or if time windows alone are the primary win. Don't build both before checking.
- **Config schema:** Define `time_windows` list shape in `FrequencyConfig` Pydantic model before Phase 1 starts.
- **Test plan:** (1) Unit test time-window binning including >24h edge case, (2) unit test calendar filtering, (3) integration test new GTFS schema through lens.py and grid.py, (4) regression check that AM peak numbers match old pipeline output.

## Phase 1 Code Review Findings (2026-04-15)

**Reviewers:** Blind Hunter (adversarial, diff-only) + Edge Case Hunter (project-aware)

### Fixed (Patch)

- [x] **P1: Unique time_window keys validator** [config.py] — Duplicate keys would cause double-counting in `restratify_for_window`. Added `field_validator("time_windows")` to `FrequencyConfig` rejecting duplicates at config load.
- [x] **P2: HH:MM format validation on TimeWindow.start/end** [config.py] — Malformed times like `"07:00:30"` would crash at runtime, not at config load. Added `field_validator("start", "end")` enforcing exactly HH:MM with range checks.
- [x] **P3: Guard against start == end (zero-length window)** [config.py] — `start == end` makes `_bin_departure` match ALL departures via the else branch. Added `model_validator` rejecting it.
- [x] **P4: Fill null route_short_name with route_id** [gtfs.py] — Some GTFS feeds have null `route_short_name`. Now falls back to `route_id` so the UI always has a display name.
- [x] **P5: Clamp inf in best_route_headway_min** [gtfs.py] — `60.0 / 0.0` produced `inf`. Now uses `pl.when(_max_route_tph > 0)` guard, producing `None` instead of `inf`.

### Deferred

- [x] **D1: No overlap/gap validation for time windows** — First-match-wins is correct for current config (windows tile 24h). A pairwise overlap validator is non-trivial and not blocking. Revisit if config becomes user-editable.
- [x] **D2: `map_elements` performance on hot path** — Two `map_elements` calls in v2 (binning + trips_per_hour). Could be vectorized with `pl.when().then()` chains. Not blocking — GTFS parse is a one-shot cached operation (~0.4s). Profile before optimizing.
- [x] **D3: Cache key collision with dashed window keys** — Current keys use underscores so no issue. Using `|` separator would harden, but not urgent.

### Dismissed (4)

- Legacy v1 path broken — false positive, `all_hex_grids` IS assigned in the else block.
- Partial cache write — already handled by AND condition in `fetch_gtfs_v2`.
- Stratified cache only has default window — by design for debugging.
- Schema version coupling — correct behavior, version reflects actual schema difference.

## Data Source

- **GTFS zip** from `https://muni-gtfs.apps.sfmta.com/data/muni_gtfs-current.zip` (fixed in previous commit)
- Contains: `routes.txt`, `trips.txt`, `stop_times.txt`, `stops.txt`, `calendar.txt`, `calendar_dates.txt` — all needed
- Muni Stops dataset (`i28k-bkz6` on DataSF) available as supplementary stop metadata if needed
