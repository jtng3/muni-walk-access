# MUNI Walk Access

Transit accessibility analysis for San Francisco. Computes what fraction of every neighborhood's residents are within a given walking time of a transit stop meeting a given frequency threshold — then makes those answers explorable on an interactive map.

**Live site**: https://sfmuni-walk-access.vercel.app/

---

## What the project does

For every residential address in San Francisco (~232,000 of them):

1. **Snap** the address to a pedestrian walk network built from OpenStreetMap.
2. **Route** to the nearest transit stop via a contracted network (pandana).
3. **Join** that stop's frequency profile from the SFMTA GTFS feed — across five time-of-day windows (AM peak, midday, PM peak, evening, overnight).
4. **Stratify** into 41 Analysis Neighborhoods and three equity-lens overlays (EJ Communities, SFMTA Equity Strategy Neighborhoods).
5. **Score** a 2D grid: `pct_within[walking_threshold][frequency_threshold]`. Emit per-neighborhood and per-H3-hex accessibility matrices as JSON.

The frontend reads the JSON and lets you slide walk-time / frequency / time-of-day / route-mode thresholds with live recoloring.

---

## Architecture

This is a monorepo with two independent halves:

```
muni-walk-access/
├── pipeline/       # Python data pipeline
│   ├── src/muni_walk_access/
│   │   ├── ingest/      # DataSF SODA, GTFS zip, OSMnx walk network
│   │   ├── network/     # pandana contracted-hierarchy build + HDF5 cache
│   │   ├── route/       # bulk nearest-stop with snap-distance correction
│   │   ├── frequency/   # calendar-aware GTFS trip-counting per time window
│   │   ├── stratify/    # spatial-join lenses + vectorized pct_within grids
│   │   └── emit/        # schema-validated JSON + GeoJSON data contract
│   ├── tests/           # pytest suite
│   ├── config.yaml      # grid axes, lenses, routing params
│   └── docs/            # design docs and methodology
└── site/            # Astro + React frontend
    ├── src/
    │   ├── components/  # Map, controls, neighborhood cards
    │   ├── data/        # Pipeline-generated JSON contracts (gitignored)
    │   └── layouts/
    └── public/
        ├── data/        # Runtime-fetched JSON (also gitignored)
        └── tiles/       # Protomaps PMTiles basemap (gitignored)
```

**Data flow**: `pipeline/` runs and emits JSON files into `site/src/data/` (gitignored). The Astro build inlines them at compile time for SSR and ships runtime-fetched copies via `site/public/data/`. The site has zero server-side dependencies — everything is static after build.

**Data contract** (`pipeline/src/muni_walk_access/emit/schemas.py`): every output file is validated with Pydantic before write. Matrix dimensions, value ranges `[0.0, 1.0]`, default-index bounds — all enforced at the boundary. The frontend can trust the shape.

---

## Engineering highlights

- **Adversarial audit in production** — `pipeline/docs/pipeline-audit-2026-04-16.md` documents a top-to-bottom correctness review (calendar handling, routing distance math, null-unreachable leakage, a 2.3× perf regression) and the fix cycle. Includes a post-verification errata where one claim was retracted after real data checking.
- **Vectorized Polars throughout** — the per-cell filter loop that dominated stratification was replaced with a single `group_by + mean` pass (~280× faster for hex grids; similar for neighborhood grids). See `stratify/grid.py`.
- **Reproducibility** — every run emits a `config_snapshot.json` with config hash, git SHA, GTFS feed sha256, OSM extract date, DataSF dataset timestamps, and an upstream-fallback flag. Anyone with the snapshot can trace the inputs.
- **TTL-aware caching layer** (`ingest/cache.py`) with conditional HTTP fetch (ETag + Last-Modified), stale-fallback on upstream failure, and content-hash keyed parsed parquets.
- **Calendar-correct GTFS** — the frequency math reads `calendar.txt` and `calendar_dates.txt`, filters by `start_date ≤ ref_date ≤ end_date`, and applies holiday exceptions. Picks the representative service date from the feed's validity window with today-priority.

---

## Running locally

Prerequisites: macOS or Linux, Python ≥ 3.12 (via [uv](https://docs.astral.sh/uv/)), Node ≥ 22.12, and system libs for the geospatial stack.

```sh
# macOS
brew install gdal geos proj
```

```sh
# Install + build once
make install
make smoke            # quick sample-mode pipeline + site build
```

```sh
# Full pipeline run (~1 min on an M-series Mac; hits DataSF + SFMTA over the network)
make pipeline-full

# Dev site
make site-dev
```

See `Makefile` for individual stages.

---

## Methodology

Short version: every SF residential address gets a "nearest stop" via pedestrian network routing, and that stop's scheduled trips-per-hour in each time window defines whether the address "meets" a given frequency threshold. Aggregate by neighborhood → pct_within. See `pipeline/docs/per-route-time-windows-plan.md` for the per-route / time-of-day extension and `site/src/components/MethodologySection.astro` for the reader-facing writeup.

---

## Portability

Currently SF-specific. Design doc `pipeline/docs/multi-city-scaling.md` captures the planned `AddressSource` + `GTFSFeed` adapter pattern for Philadelphia (next city) and the data-source inventory beyond that.

---

## Credits

Data: [DataSF](https://data.sfgov.org), [OpenStreetMap](https://openstreetmap.org), [SFMTA GTFS](https://www.sfmta.com/reports/gtfs-transit-data).
Built by Jaeger Tang. Luke Armbruster, collaborator.

## License

MIT — see [LICENSE](./LICENSE).
