# Pipeline Logic & Code Quality Audit

**Date**: 2026-04-16
**Scope**: Top-to-bottom audit of `pipeline/src/muni_walk_access/` (3,922 LoC across ingest, network, route, frequency, stratify, emit) plus `tests/`, against `config.yaml`. Verified by running the test suite.
**Goal**: Identify math/logic bugs, accuracy approximations, performance regressions, and hardening work that improves the pipeline today and lowers the cost of porting to other cities (Philadelphia is city #2; see `multi-city-scaling.md`).

**Headline (post-verification, see errata below)**:
- **A1** (GTFS calendar): near-miss; verified against the real MUNI feed — zero impact on current deployment because all holiday exceptions in the cached feed expired before the last build. Real risk at the next holiday (Memorial Day 2026-05-25).
- **A2** (unreachable-address leak): trivial impact — ~5 rows in the 232k-address download parquet have wrong distance/walk_minutes. Does not affect grid headline, map colors, or neighborhood pages.
- **B1** (trips/hour vs headway): accuracy approximation; partially addressed by the site's methodology page.
- **B5** (missing neighborhoods): **RETRACTED** — claim was based on stale memory. Verified: the last deployed build (`site/dist/data/grid_am_peak.json`, `run_id 2026-04-16T08:02:13Z`) has all 41 neighborhoods with real populations totaling 232,463. See "Errata" immediately below.
- **C** (perf regression in `compute_grid`): ~12,600 filter passes per run. Fixed in Phase 1.
- **D** (test rot): 14 of 180 tests failing from drifted signatures. Fixed in Phase 1.
- The architecture is *already* close to city-portable; the audit ratifies the existing scaling doc and adds 8 hardening recommendations (E).

---

## Errata (2026-04-16, post-verification)

**B5 is retracted**. The original audit claimed 11 of 41 neighborhoods were dropped from the live deployment due to the `SRES`/`MRES` parcel filter, and that Russian Hill, Chinatown, Presidio, etc. would be absent. Verification against `site/dist/data/grid_am_peak.json` (the last actual build output) shows **all 41 neighborhoods present**, including every one the audit named as missing:

| Neighborhood the audit said was missing | Actual pop in deployed build |
|---|---|
| Russian Hill | 6,060 |
| Chinatown | 1,866 |
| Presidio | 30 (federal land — plausibly low) |
| Treasure Island | 7 (former military — plausibly low) |
| Golden Gate Park | 2 (park — plausibly low) |
| Lincoln Park | 51 |
| McLaren Park | 89 |
| Seacliff | 923 |
| Lakeshore | 1,750 |
| Presidio Heights | 3,307 |
| Glen Park | 3,731 |

**Root cause of the bad claim**: the audit repeated a stale internal note that described an earlier pipeline state, and confused `site/src/data/*.json` (gitignored local source, which had been overwritten with a `--sample 100` run) with `site/dist/data/*.json` (the actual last-build output). Verification rule going forward: always confirm against `site/dist/data/` and `config_snapshot.json` before making deployment-impact claims.

**What the current parcel filter genuinely does**: some neighborhoods have low residential populations (Presidio, Treasure Island, Golden Gate Park) because they are mostly non-residential land. This is realistic, not a bug. The original memory-note observation may have reflected either an even earlier filter or a different analysis; either way, it does not describe current deployment state.

**All other audit findings (A1, A2, B1-B4, C, D, E1-E8) remain valid as stated** — they were derived from reading the current source code + running the current test suite, not from memory. Only B5 was built on stale ground-truth.

---

---

## Triage update (2026-04-16, post-verification pass)

An independent verification pass re-read the cited code to confirm each finding. Corrections to the original audit:

| Finding | Original verdict | Corrected verdict | Reason |
|---|---|---|---|
| **A2** Unreachable-address leak | "Correctness bug — affects SF outputs today" | **Data-hygiene bug; does not affect published grid numbers today** | Pandana returns `maxdist=5000` (not NaN) for unreachable nodes. Leaked `walk_minutes ≈ 63 min` — well above the 15-min max walk threshold. Grid outputs are clean; the leak pollutes raw `stratified.parquet` exports and the mean/median distance logs. Still worth fixing. |
| **B1** trips/hour vs headway | "Document the assumption" | **Partially addressed** — `site/src/components/MethodologySection.astro:56-60` already explains aggregate vs single-route. Add one sentence about within-window spacing assumption. | User-facing framing is honest; the pitfall is narrower than the audit implied. |
| **B4** Boundary sjoin non-determinism | "Real risk, breaks reproducibility test" | **Theoretical only** — pandas ≥2 RTree is deterministic in practice; `test_identical_runs_produce_identical_output` exercises `compute_grid`, not `aggregate_to_lenses`. | Opportunistic hardening, not load-bearing. |
| **C** `compute_grid` perf regression | "~1,680 filter passes" | **~12,600 filter passes** | `compute_grid` is called 10× per run (5 time windows × 2 metrics: aggregate + headway), not 1×. Bigger win than originally stated; moves up in priority. |

**Findings missed in the original audit**:

1. `_OSM_DATASET_ID = "osm-sf-pedestrian"` (`ingest/osm.py:20`) is SF-specific — add to E4's lift-to-config list.
2. `_datasf_timestamps` module-global (`ingest/datasf.py:32`) has the same cross-run contamination risk as `_upstream_fallback` — roll into the E6 `RunContext` refactor.
3. `_bin_departure` (`ingest/gtfs.py:210, 301`) is *also* a per-row Python `map_elements` callback on ~1.8M `stop_times` rows — add to the vectorization list in section C.
4. **Post-A2 drift risk**: once unreachable addresses get `null` `nearest_stop_id`, `restratify_for_window` (`stratify/lens.py:291-302`) will silently propagate nulls into `trips_per_hour_peak`. Downstream `compute_grid` needs to handle that case cleanly (it currently filters on `>= trips_needed`, which is False for null — likely correct, but should be explicit).

All other findings (A1, B2, B3, D, E1-E8 except the missed items above) are confirmed exactly as stated. B5 was later retracted (see Errata).

---

## A. Correctness — must-fix bugs

### A1. GTFS calendar handling is incomplete

**Location**: `ingest/gtfs.py:53-89` (`_get_service_ids`), `ingest/gtfs.py:134-142` (filter application).

**Symptom**: `_get_service_ids` reads only `calendar.txt`. It does **not** read `calendar_dates.txt` (GTFS exception/holiday table) and does **not** filter by `start_date` / `end_date`.

**Why it matters** — two real consequences:

1. **Overlapping service definitions inflate trip counts.** When MUNI publishes a transitional feed where an old "Fall 2025" service and a new "Spring 2026" service both have `monday=1 … friday=1` set, both `service_id`s get included. Every trip from both services flows into the per-stop departure count → trips/hour roughly doubles → headways halve → the "served" pct goes up artificially. The fix requires checking that *today's date* (or a representative date inside the feed's validity window) falls between `start_date` and `end_date`.
2. **`calendar_dates.txt`-only feeds silently include all trips.** If a feed encodes service entirely via `calendar_dates.txt` (no `calendar.txt` at all — common for some publishers), the code logs a warning and returns an empty `service_ids` set. Then `if service_ids:` at line 134 is `False` and **filtering is skipped entirely** → weekend, holiday, and special-event trips all count toward weekday peak frequency.

Even when `calendar.txt` is present, holidays and snow-day exceptions encoded via `calendar_dates.txt exception_type=2` are not removed, so peak frequencies on holiday weeks would include cancelled service.

**Why this hasn't been caught**: SF MUNI happens to ship a clean `calendar.txt` with non-overlapping services for stable weeks, so day-to-day runs look fine. SEPTA (Philly) uses overlapping services and aggressive `calendar_dates.txt` exceptions; this bug **will** manifest as wrong numbers immediately on city #2.

**Fix** (standard approach used by transitfeed, gtfs-kit, OpenTripPlanner):

1. Pick a representative service date — read `feed_info.feed_start_date` / `feed_end_date` and use the median Wednesday in the window, or fall back to today if those are absent.
2. From `calendar.txt`, find rows where `start_date ≤ date ≤ end_date` AND `day_of_week_column = 1`.
3. Apply `calendar_dates.txt`: add `service_id`s with `exception_type=1` for that date, remove `service_id`s with `exception_type=2`.
4. The resulting set is the active services for that date. Use it to filter `trips.txt`.

This also means `calendar_dates.txt` becomes a required column in any `GTFSSource` adapter contract (see E2).

---

### A2. Unreachable addresses leak finite distances and walk_minutes

**Location**: `route/nearest_stop.py:144-176`.

**Symptom**: When pandana finds no POI within `_MAX_DIST_METERS` for a network node, `raw_poi_idx` returns NaN. The code correctly nulls `nearest_stop_id` for those rows:

```python
addr_poi_idx = np.where(reachable, raw_poi_idx, 0).astype(int)  # 0 for unreachable
nearest_stop_ids = [stop_ids[i] if r else None for i, r in zip(addr_poi_idx, reachable)]
```

But `addr_distances` is computed *unconditionally*:

```python
addr_distances = network_distances + addr_snap_m + stop_snap_m[addr_poi_idx]
```

For unreachable rows, `addr_poi_idx` was forced to 0, so they get `stop_snap_m[0]` — the snap of *whichever stop happened to be index 0 in the array* — added to whatever pandana returned for `network_distances` (NaN or `maxdist` depending on pandana version). The result is a finite number that:

- Passes the `null_count == 0` integrity check in `__main__.py:121-124`.
- Gets multiplied by `pace_min_per_mile` to produce a finite `walk_minutes`.
- Silently does or does not meet the walk threshold in `compute_grid`, based on garbage.

**Compounding issue — the clamp warning is wrong direction**: line 179 checks `(addr_distances >= _MAX_DIST_METERS).sum()`, but `addr_distances` is post-snap. A truly unreachable address with `network_distances = 5000` plus `addr_snap_m = 50` plus `stop_snap_m[0] = 30` reports as 5080 — flagged. A near-clamp address at `network_distances = 4900` plus `addr_snap_m = 60` plus `stop_snap_m = 50` reports as 5010 — also flagged but legitimately reachable. The clamp warning conflates two different conditions.

**Why this hasn't been caught**: only ~5 SF addresses hit the clamp per full run (per memory `project_pipeline_runtime_findings.md`), so the impact on aggregate city stats is sub-percent. But for a smaller area, an outer city, or a sparse rural-ish neighborhood, the wrong-distance leak could shift the result materially.

**Fix**:
- Compute `network_distances` and clamp-check it directly, *before* adding snaps.
- For `~reachable` rows, set `addr_distances`, `walk_minutes`, and `nearest_stop_id` all to `None`.
- Update `_check_routing_integrity` to expect (and log) some null distances rather than erroring on them.
- Consider distinguishing "unreachable: no stop within `maxdist`" from "snap failure: address didn't resolve to a network node" in the logs.

**Related**: `_MAX_DIST_METERS = 5000.0` is a top-of-file constant. Move it to `config.routing.max_distance_m` (or similar). SF's hilly geography, Philly's flat grid, and a future Boulder run will all want different values.

---

## B. Accuracy — document or refine

### B1. "Trips/hour" is conflated with "headway" in the aggregate metric

**Location**: `stratify/grid.py:47` (`_compute_neighbourhood_grid` aggregate branch).

```python
trips_needed = 60.0 / f_thresh
freq_expr = pl.col("trips_per_hour_peak") >= trips_needed
```

This treats *average frequency over the window* as if it were an upper bound on wait time. A stop with 6 trips/hour can have a 30-minute mid-window gap and still pass a "10-minute headway" threshold. That's because trips_per_hour is `count(trips) / window_hours` — it does not encode spacing.

The headway scoring mode you added (`best_route_headway_min` via `60 / max_per_route_tph`) is closer to what users imagine, but it still assumes uniform spacing within the window for the highest-frequency route.

**Why it matters in practice**: For SF MUNI most trunk routes are uniformly spaced and the approximation is tight. For SEPTA's outer regional rail (where a 4-trip-per-hour window can be three trips clustered in the first 20 minutes and then nothing for 40 minutes), the metric will overstate accessibility.

**Why this isn't a "bug"**: The grid label says "Frequency" and there is precedent in transit planning for using trips/hour as a proxy. The pitfall is that users read "10-min headway = wait at most 10 minutes" — which the metric doesn't promise.

**Two fix options** (pick one):

1. **Cheap**: document the assumption in the data-contract version notes and in the on-page methodology section. Note that `trips_per_hour_peak >= 60/H` is a *necessary but not sufficient* condition for "headway ≤ H minutes."
2. **Better**: compute true scheduled headway per `(stop_id, route_id, time_window)`. Sort departure times, take consecutive gaps, take the max gap. Aggregate to stop-level by minimum across routes. This gives a "max wait you'll observe" metric that matches user intuition.

The headway scoring mode infrastructure is already in place — extending the per-route headway computation is a few-line addition to `_compute_stop_frequencies_v2`.

---

### B2. Snap-distance correction adds Euclidean to network distance

**Location**: `route/nearest_stop.py:149-176`.

```python
total = address_snap_euclidean + network_node_to_node + stop_snap_euclidean
```

Adding straight-line snap distances to a sidewalk-network distance is a defensible approximation, but it's directional:

- **Over-estimates** when the snap is roughly parallel to the sidewalk that the address would actually walk on (the snap distance is *also* walked, but along the network).
- **Under-estimates** when the snap requires walking around a building or down a driveway not represented in OSM.

For SF's grid streets and typical ~30-50m snap distances, the error is probably ±5m. For a sprawling parking lot or a campus, it could be ±100m.

**Recommendation**: leave the math as is (the alternative — projecting the snap onto the nearest edge geometry — is much more code) but add a one-line comment in the function explaining the tradeoff, and consider logging snap-distance percentiles per run so a future operator can sanity-check that snaps stay small.

---

### B3. Hardcoded EJ filter is SF-specific

**Location**: `stratify/lens.py:31` (`_EJ_SCORE_THRESHOLD = 21`), `stratify/lens.py:96-115`.

The SF dataset is a numeric `score` column and the filter is `score >= 21` (top 1/3 of cumulative burden, scores 21–30). PennEnviroScreen uses an `EJAREA = "yes"` text flag — completely different schema. Boston's MassGIS uses still another. The current code branches on `if lens.id == "ej_communities"`, which doesn't generalize.

**Fix**: lift the filter into the `LensConfig` schema. Add optional fields:

```python
class LensConfig(BaseModel):
    id: str
    label: str
    source: SourceConfig                 # was: datasf_id
    name_field: str = "name"             # was: hardcoded _ANA_NAME_COL
    filter_field: str | None = None
    filter_op: Literal["eq", "ne", "gte", "lte", "in"] | None = None
    filter_value: str | int | float | list[str] | None = None
```

Then the lens loader applies the filter generically.

---

### B4. Boundary-coincident addresses get non-deterministic neighborhood assignment

**Location**: `stratify/lens.py:188`.

```python
ana_joined = gpd.sjoin(addr_gdf, ana_slim, how="left", predicate="within")
ana_joined = ana_joined[~ana_joined.index.duplicated(keep="first")]
```

When an address point falls exactly on a polygon boundary (or in a gap between adjacent polygons that touch but don't share rings), `gpd.sjoin` returns multiple rows for that address. `keep="first"` picks whichever row appeared first in the spatial-index traversal — which is non-deterministic across runs depending on RTree node ordering.

**Why it matters**: only a few addresses hit this, but it breaks the "identical inputs → identical outputs" property that `test_identical_runs_produce_identical_output` (`tests/test_stratify.py:511`) explicitly asserts.

**Fix**: sort the sjoin result by `(addr_index, neighborhood_id)` before deduplicating, so the dedup picks the lexicographically-first neighborhood. Alternatively use `predicate="intersects"` and tiebreak by nearest centroid.

---

### B5. Missing-neighborhoods claim — RETRACTED

**Original claim**: 11 of 41 Analysis Neighborhoods were silently dropped by the pipeline due to the `SRES`/`MRES` parcel filter being too narrow; the frontend could not render them because they weren't in the data.

**Verification showed this is wrong for the current deployment.** See the Errata section above. The last deployed build has all 41 neighborhoods. Low-population neighborhoods (Presidio 30, Treasure Island 7, Golden Gate Park 2) are low because those are mostly non-residential land, not because of a filter bug.

**Latent design concern that still applies** (unchanged from the original audit, just no longer urgent):

- `compute_grid` (`stratify/grid.py:96`) iterates `stratified["neighborhood_id"].unique()`, not an enumeration of the boundaries roster.
- **If** a future pipeline change caused a neighborhood to have zero residential addresses, it would be silently absent rather than rendered as "no data." The current output happens to include all 41, but the code path for "neighborhood has zero addresses → silently absent" remains.

**Optional hardening (not urgent)**: in `compute_grid`, left-join the neighborhood list from the Analysis Neighborhoods boundary dataset onto the grouped result, so any neighborhood that would have been silently dropped instead appears with `population=0` and all-zero `pct_within`. This is insurance against future parcel-filter changes, not a fix for a current bug.

---

## C. Performance — your own pattern regressed

**Location**: `stratify/grid.py:96-128` (`compute_grid`).

The hex path (`compute_hex_grids`, lines 226-294) was already converted to the vectorized `meets_exprs + group_by + agg` pattern that you measured at 280× speedup in Story 1.11 (memory: `learning_vectorize_over_filter_loops.md`). The neighborhood path was not.

`compute_grid` iterates neighborhoods in Python and runs `nbhd_df.filter(freq_expr & walk_expr).height` for every (freq, walk) pair:

- ~40 neighborhoods × 7 frequency thresholds × 6 walk thresholds = **1,680 Polars filter passes per run**.

Apply the same `meets_exprs + group_by("neighborhood_id") + mean()` pattern that the hex code already uses. It's a few-line change, mechanical translation, and should bring `compute_grid` time down to single-digit seconds.

**Other perf wins worth noting:**

- `_compute_stop_frequencies_v2` (`ingest/gtfs.py:299, 339`) uses `map_elements` for departure-time binning and trip-count division. Both are per-row Python callbacks. With ~1.8M `stop_times` rows in the MUNI feed, this is the dominant cost in GTFS parsing. Both can be vectorized with `pl.when().then()` chains.

- `assign_hex_cells` (`stratify/grid.py:176-186`) does `[h3.latlng_to_cell(lat, lon, res) for lat, lon in zip(...)]` × 5 resolutions × ~232k addresses = **~1.16M Python-level h3 calls**. h3-py 4 has batch APIs (`h3.latlng_to_cells` or numpy-friendly variants in some builds). At minimum, fuse the loop so each lat/lon pair only gets unpacked once instead of five times.

- `compute_grid` city-wide accumulator (lines 116-137) multiplies *pre-rounded* `pct_within` by `population` and divides by `total_pop`. This re-introduces rounding error in the headline number. Compute city-wide directly from the global df (the same `meets_exprs + mean()` pattern, no group_by) — one extra reduction, no rounding artefacts.

---

## D. Test rot — 14 failures masking real risk

```
14 failed, 166 passed in 41.26s
```

Failure clusters:

| Cluster | Failures | Cause |
|---|---|---|
| `fetch_gtfs` 2-tuple unpacking | 4 | Production now returns `(df, sha256, feed_date)`; tests still unpack `(df, sha256)`. |
| `write_config_snapshot` signature | 5 | Required `gtfs_feed_date` and `datasf_data_updated` were added; tests don't supply them. |
| `ConfigSnapshot` schema additions | 2 | Same field additions, validating from dict literals that no longer satisfy the model. |
| `test_main` orchestration | 3 | The v2 multi-window path was added without updating the orchestration tests. |

**These were broken when you added time-windowed GTFS, lens timestamps, and the v2 data contract.** The pipeline runs end-to-end successfully (memory: `project_pipeline_runtime_findings.md` documents 1.75-min full runs), so the production code is correct and the *tests* are stale.

**Why this is urgent before Philly**: the test rot hides any new regressions you introduce while building the address-source adapter and GTFS adapter. If a Philly test fails, you'll have to dig to figure out whether (a) your refactor broke something, (b) the test was already broken, or (c) the test is correct and a city-specific assumption broke. Green baseline before refactor.

**Effort**: ~1-2 hours. The fixes are mechanical signature updates.

---

## E. Hardening for portability — what the scaling doc gets right, what to add

The existing `pipeline/docs/multi-city-scaling.md` is a strong plan. The audit confirms its core architectural calls and surfaces additional hardening items.

### E0. What the audit confirms from the scaling doc

| Scaling-doc claim | Audit confirms because… |
|---|---|
| AddressSource Protocol is the right boundary | `fetch_residential_addresses` is the only city-coupled function in `datasf.py`. Clean refactor target. |
| Lens config should carry `name_field` and `filter_*` | `_ANA_NAME_COL = "nhood"` (line 28) and `_EJ_SCORE_THRESHOLD = 21` (line 31) are the only hardcoded knobs in `lens.py`. Trivial to lift. |
| SEPTA zip-in-zip is a real gap | `_compute_stop_frequencies_v2` reads zip entries directly with `zf.read("trips.txt")`; would crash on the outer wrapper. |
| Site is already largely city-agnostic | The data contract is per-city-runnable; only `MapInner.tsx` bounds and the PMTiles file are SF-specific. |

### E1. Define a canonical address schema as a Pydantic model

The Protocol comment says `address_id, longitude, latitude, is_residential, [optional: use_code]`, but it's not enforced. Make it explicit:

```python
class ResidentialAddress(BaseModel):
    address_id: str
    longitude: float
    latitude: float
    is_residential: bool
    use_code: str | None = None        # source-specific category
    parcel_id: str | None = None       # for joins back to parcel data
```

Validation happens at the adapter boundary. Every city implementation returns a `pl.DataFrame` whose columns conform to this schema (or a `list[ResidentialAddress]` that gets coerced). This is the *input* analog of `emit/schemas.py` for outputs.

### E2. Define a canonical GTFS schema as a Pydantic model

SEPTA's zip-in-zip is one of *several* GTFS oddities you'll hit. Other agencies bundle multiple feeds, ship `shapes.txt` extensions, omit `route_short_name`, or ship UTF-8-BOM CSVs. Push the unwrap/normalize logic behind a `GTFSSource` Protocol that returns flat, validated DataFrames:

```python
class GTFSFeed:
    trips_df: pl.DataFrame             # required cols enforced
    stop_times_df: pl.DataFrame
    stops_df: pl.DataFrame
    routes_df: pl.DataFrame
    calendar_df: pl.DataFrame | None   # nullable per spec
    calendar_dates_df: pl.DataFrame | None
    feed_info_df: pl.DataFrame | None
    feed_sha256: str
    feed_date: str
```

Each city's `GTFSSource` knows how to fetch the feed and unpack any city-specific wrapping. The downstream frequency math (`_compute_stop_frequencies_v2`) takes a `GTFSFeed` and stays untouched. **This is also the natural place to enforce the calendar fix from A1**: `calendar_dates_df` becomes part of the contract, and the frequency code consumes it.

### E3. Coordinate-system contract

Every adapter must promise WGS84 (EPSG:4326). Add an explicit assertion at the adapter boundary:

```python
def validate_wgs84(lats: pl.Series, lons: pl.Series) -> None:
    if lats.min() < -90 or lats.max() > 90 or lons.min() < -180 or lons.max() > 180:
        raise ValueError("Coordinates outside WGS84 range — adapter must reproject")
```

Silent CRS mismatches (e.g. a city ships addresses in NAD83 / state-plane) would produce plausible-looking but wrong walk distances.

### E4. Per-city tunable constants

Lift these out of source code and into config:

| Current location | Constant | Why per-city |
|---|---|---|
| `route/nearest_stop.py:17` | `_MAX_DIST_METERS = 5000.0` | Dense-grid cities want smaller; mountain towns want larger. |
| `route/nearest_stop.py:16` | `_METERS_PER_MILE = 1609.34` | International is 1609.344, US survey is 1609.347 — pedantic but visible at scale. |
| `stratify/lens.py:31` | `_EJ_SCORE_THRESHOLD = 21` | SF-specific (CalEnviroScreen). Should be in `LensConfig.filter_value`. |
| `stratify/lens.py:28` | `_ANA_NAME_COL = "nhood"` | Should be in `LensConfig.name_field`. |
| `emit/grid_hex_json.py:21` | `_EXPECTED_CELL_COUNTS` | Per-city sanity bounds (Philly's bbox produces different cell counts). |
| `__main__.py:405` | `output_dir = Path(__file__).parent.parent.parent.parent` | Add `--output-dir` CLI flag for `site_sf/`, `site_philly/`. |

### E5. Provenance file naming assumes SF dataset IDs

`_record_timestamp` (`ingest/datasf.py:82`) parses `<dataset-id>-YYYYMMDD.<ext>` from the cache filename. SEPTA's GTFS doesn't have a dataset-ID format that matches this pattern, and `_META_FILENAME = "muni-gtfs-http.json"` (`ingest/gtfs.py:29`) is a hardcoded literal.

**Fix**: each adapter supplies its own `dataset_id` string used by both the cache layer and provenance. Make `CacheManager` agnostic — it already mostly is, but the timestamp-extraction regex assumes a specific stem format.

### E6. Global module-state for upstream-fallback flag

`_upstream_fallback` lives at module scope in `ingest/datasf.py:31` and is mutated by `set_upstream_fallback()`. Both `gtfs.py` and `osm.py` import that setter. With multiple cities running in the same process (a future operator might run SF then Philly in one Python session), the flag would cross-contaminate.

**Fix**: replace with a small `RunContext` dataclass passed through the pipeline:

```python
@dataclass
class RunContext:
    run_id: str
    config: Config
    cache: CacheManager
    upstream_fallback: bool = False
    datasf_timestamps: dict[str, str] = field(default_factory=dict)
```

Pipeline functions take `ctx: RunContext` instead of reaching into module globals. This also makes testing cleaner — no `_reset_fallback()` hooks.

### E7. Hardcoded slug source-of-truth for cross-output joins

`slugify_neighborhood` in `stratify/lens.py:34` is the single source of truth for neighborhood IDs, and it's imported by `emit/geojson.py:18` to look up boundary geometries. This works because the same function produces both the IDs in `grid.json` and the IDs in `neighborhoods.geojson`.

The audit isn't flagging a bug here — the design is correct. But for portability, document this contract explicitly in a comment block on `slugify_neighborhood`. Future-you porting to Philly will be tempted to "improve" the slug rules, and that would silently break the geojson-to-grid join.

### E8. CSV reads use `infer_schema_length=0` everywhere — strict casts will explode on dirty data

`fetch_tabular` (`ingest/datasf.py:127`) and the GTFS readers all use `infer_schema_length=0` so every column is read as String, then explicitly cast downstream. This is the right call for SODA (mixed-type pages). But the casts are unguarded:

```python
pl.col("stop_lat").cast(pl.Float64)
```

If a row has `stop_lat = ""` or `stop_lat = "NULL"`, this raises and crashes the pipeline. SF MUNI happens to ship clean coordinates; SEPTA and OPA don't always.

**Fix**: use `cast(..., strict=False)` to coerce parse failures to null, then explicitly filter out null coordinates with a logged count. The existing pattern at `gtfs.py:186-194` (filter out null coords with a warning) is the right shape — apply it consistently.

---

## F. What's solidly built — don't touch

These pieces are good as-is and shouldn't be disturbed during the multi-city refactor:

- **Cache layer (`ingest/cache.py`)**: TTL-aware, conditional-fetch (ETag + Last-Modified for GTFS), stale-fallback on upstream failure, content-hash keyed for parsed parquets so a re-parse with different params doesn't collide. Good design.
- **Pydantic output schemas (`emit/schemas.py`)**: tight matrix-shape validation in `validate_grid_structure` and `validate_hex_grid_structure`, range checks on every percentage field. Catches a lot at the boundary and gives the data contract real teeth.
- **Run provenance (`emit/config_snapshot.py` + `_get_git_provenance`)**: config hash, git SHA, git tag, GTFS sha256, GTFS feed date, OSM extract date, DataSF dataset timestamps, upstream-fallback flag. Reproducibility story is strong — anyone with `config_snapshot.json` can in principle reproduce the run.
- **TimeWindow validation (`config.py:39-64`)**: proper overnight-wrap handling in `duration_hours`, rejects zero-length windows, rejects duplicate keys. The math is right.
- **Frequency bin classifier (`frequency/classify.py`)**: clean, validates the bin list at startup (catch-all required, ordering enforced), uses fully-vectorized `pl.when().then()` chains. Reference for how the rest of the stratify code should look.
- **Pandana network cache keyed on OSM extract date (`network/build.py:35`)**: smart — invalidates cleanly when the underlying graph changes, no manual cache-bust required.

---

## G. Fix plan (post-triage)

Ordered by (a) green-baseline first, (b) biggest correctness/perf win per hour, (c) what unblocks the Philly refactor. Steps 1-4 stay in the current session with its primed audit context; steps 5+ start fresh sessions per cluster with the green baseline from step 1.

### Phase 1 — in this session (~6-8h total)

**Fix #1 — Repair 14 failing tests (D).** ETA 1-2h.

Target: `pytest -q` returns 0 failures. Changes are mechanical signature updates to tests; no production code touched.

- `tests/test_ingest.py` — 4 tests unpack `fetch_gtfs(...)` as 2-tuple; production returns 3-tuple `(df, sha256, feed_date)`. Update the unpacks.
- `tests/test_emit.py` — 5 tests call `write_config_snapshot(**self._snapshot_kwargs(tmp_path))`; helper missing `gtfs_feed_date` and `datasf_data_updated`. Add both to the helper's dict literal.
- `tests/test_schemas.py` — 2 tests construct `ConfigSnapshot` from a dict that doesn't include `data_versions.gtfs_feed_date` and `data_versions.datasf_data_updated`. Add both to the fixture.
- `tests/test_main.py` — 3 orchestration tests broken by the v2 path. Read the v2 branching in `__main__.py:291-395`; update mocks to match the new call signatures.

Acceptance: `cd pipeline && uv run pytest -q` → `180 passed`.

**Fix #2 — Vectorize `compute_grid` (C).** ETA 1h. Moved up from #4 after triage revealed it's ~12,600 filter passes per run, not 1,680.

Target: `stratify/grid.py:57-150` uses the same `meets_exprs + group_by + agg` pattern already present in `compute_hex_grids` (lines 226-294). Delete `_compute_neighbourhood_grid`.

Shape:
```python
# Build N_freq × N_walk boolean columns once (vectorized)
# group_by("neighborhood_id").agg(pl.col(name).mean() for each)
# Iterate the result in Python only to shape into NeighborhoodGrid models
```

Also compute city-wide directly from the global df (one extra reduction, no rounding-error re-aggregation).

Acceptance: identical output vs pre-refactor on a fixed input (compare `grid_am_peak.json` byte-for-byte after a re-run); `test_identical_runs_produce_identical_output` still green.

**Fix #3 — GTFS calendar correctness (A1).** ETA 3-4h.

Target: `_get_service_ids` (`ingest/gtfs.py:53-89`) becomes `_get_active_service_ids(zf, service_date)` that:

1. Reads `feed_info.txt` if present to pick a representative date inside `feed_start_date`/`feed_end_date`; else uses the modal weekday in `calendar.txt`'s range; else today.
2. Reads `calendar.txt`: keep rows where `start_date ≤ date ≤ end_date` AND `day_of_week_column(date.weekday()) == "1"`.
3. Reads `calendar_dates.txt` (optional): add service_ids with `exception_type=1` for `date`; remove service_ids with `exception_type=2`.
4. Returns the resulting set.

Both `_compute_stop_frequencies` and `_compute_stop_frequencies_v2` call the new function. Handle the "no `calendar.txt`, only `calendar_dates.txt`" case — today's code silently includes all trips; new behavior must error or filter correctly.

Acceptance: add a unit test with a fixture GTFS zip containing overlapping services + a `calendar_dates.txt` exception; verify only the target-date-active trips are counted. Re-run full pipeline; compare headline pct before/after (expect small change in SF — MUNI feed is clean — but verify direction is plausible).

**Fix #4 — Unreachable address nulls + downstream handling (A2).** ETA 1h.

Target: `route/nearest_stop.py:141-215` sets `nearest_stop_distance_m = None`, `walk_minutes = None`, `nearest_stop_id = None` for rows where `~reachable`.

Changes:
1. Compute reachability first: `reachable = ~np.isnan(raw_poi_idx)`.
2. Compute `network_distances_raw` from pandana (may be NaN or maxdist); use `np.where(reachable, network_distances, np.nan)`.
3. Clamp-check against `network_distances_raw >= maxdist`, not the post-snap total.
4. Apply snap correction only to reachable rows; unreachable get `nan` through to output.
5. Update `_check_routing_integrity` (`__main__.py:118-144`) to log null counts instead of raising.
6. Verify `restratify_for_window` (`stratify/lens.py:291`) handles null `nearest_stop_id` cleanly — the left-join will produce null `trips_per_hour_peak`, and `compute_grid`'s `>= trips_needed` filter returns False on null (desired behavior). Add explicit `.fill_null(False)` in the grid refactor from #2 for clarity.

Also: lift `_MAX_DIST_METERS` to `config.routing.max_distance_m` (default 5000). Touch the config schema and `config.yaml` while we're here.

Acceptance: `tests/test_route.py` — update `test_warning_logged_when_addresses_hit_maxdist_clamp` to assert nulls on unreachable rows; add a test confirming reachable rows are unaffected. Full-pipeline re-run: verify headline pct is within rounding of the previous run (the 5 SF clamped addresses should produce same result either way).

### Phase 2 — new sessions per cluster

**Fix #5 — Define `ResidentialAddress` + `GTFSFeed` Pydantic contracts (E1, E2).** ETA 1d. New session with green baseline from #1-4.

**Fix #6 — Address-source adapter refactor + boundaries abstraction.** ETA 1-2d per `multi-city-scaling.md`.

**Fix #7 — SEPTA + OPA + PennEnviroScreen end-to-end.** ETA 2-3d per scaling doc.

**Fix #8 — `RunContext` to replace `_upstream_fallback` and `_datasf_timestamps` globals (E6 + missed #2).** ETA 2h. Last; easier once adapters have clear boundaries.

### Opportunistic lifts (do as you touch each file during 5-7)

- `_MAX_DIST_METERS` → config (done in #4)
- `_EJ_SCORE_THRESHOLD` → `LensConfig.filter_value`
- `_ANA_NAME_COL` → `LensConfig.name_field`
- `_EXPECTED_CELL_COUNTS` → per-city config or computed from bbox
- `_OSM_DATASET_ID` → derived from `config.networks.osm_place` (slugified)
- `_META_FILENAME` → derived from adapter-supplied `dataset_id`
- Output dir hardcode (`__main__.py:405`) → `--output-dir` CLI flag

---

## Appendix — file-by-file inventory

| File | LoC | Audit verdict |
|---|---|---|
| `__main__.py` | 583 | Orchestration; v2 path adds branching that complicates the test harness. Consider extracting v1/v2 into separate functions to clarify. |
| `config.py` | 226 | Strong validation. Add `address_source` config block per scaling doc. |
| `exceptions.py` | 24 | Clean. |
| `ingest/cache.py` | 98 | Solid. See E5 for the dataset-id format coupling. |
| `ingest/datasf.py` | 288 | SF-specific by design. Refactor to one of N implementations behind `AddressSource` Protocol. Module-state cleanup (E6). |
| `ingest/gtfs.py` | 676 | A1 (must-fix). Two near-duplicate `fetch_gtfs` functions (DRY); merge them. `service_days` config is plumbed but `peak_window` is unused in v2. |
| `ingest/osm.py` | 107 | Good. `osm_place` already in config — generalizes cleanly. |
| `network/build.py` | 73 | Good. Pandana cache keyed on OSM date is smart. |
| `route/nearest_stop.py` | 214 | A2 (must-fix). Lift hardcoded constants to config (E4). |
| `frequency/classify.py` | 89 | Good reference for how stratify code should look. |
| `stratify/grid.py` | 294 | C (perf regression in `compute_grid`). Hex path is exemplary. |
| `stratify/lens.py` | 340 | B3, B4, B5. Source-of-truth slug function is good design. |
| `emit/schemas.py` | 269 | Strong. Mirror this rigor for input contracts (E1, E2). |
| `emit/config_snapshot.py` | 59 | Good. |
| `emit/grid_json.py` | 68 | Clean. |
| `emit/grid_hex_json.py` | 116 | E4: per-city expected cell counts. |
| `emit/geojson.py` | 110 | Good. |
| `emit/downloads.py` | 80 | Clean. |
| `emit/docs.py` | 143 | Internal docs writer; not audited in depth. |

**Total**: 3,922 LoC. Main risk areas concentrated in `ingest/gtfs.py` (calendar correctness) and `route/nearest_stop.py` (unreachable leakage). Stratify and emit are in good shape modulo the perf regression and SF-specific knobs.
