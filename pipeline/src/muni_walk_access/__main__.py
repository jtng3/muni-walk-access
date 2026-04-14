"""Entry point for the muni-walk-access pipeline."""

from __future__ import annotations

import argparse
import io
import logging
import platform
import sys
import time
import tracemalloc
from datetime import date
from pathlib import Path

import polars as pl
import yaml
from pydantic import ValidationError

from muni_walk_access.config import Config, load_config
from muni_walk_access.emit.schemas import CityWide, NeighborhoodGrid
from muni_walk_access.exceptions import IngestError, NetworkBuildError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import fetch_residential_addresses
from muni_walk_access.ingest.gtfs import fetch_gtfs
from muni_walk_access.network.build import build_network
from muni_walk_access.route.nearest_stop import route_nearest_stops
from muni_walk_access.stratify.grid import compute_grid
from muni_walk_access.stratify.lens import aggregate_to_lenses, compute_lens_flags

logger = logging.getLogger(__name__)

# Absolute path to pipeline/docs/ — stable regardless of cwd
_DOCS_DIR = Path(__file__).parent.parent.parent / "docs"


def _write_timing_doc(
    *,
    t_network: float,
    t_addresses: float,
    t_gtfs: float,
    t_routing: float,
    t_lens: float,
    t_grid: float,
    t_total: float,
    peak_mb: float,
    address_count: int,
    stop_count: int,
    result_count: int,
    sample_mode: bool,
    sample_n: int | None,
) -> None:
    """Write timing spike markdown to pipeline/docs/timing-spike-{date}.md."""
    today = date.today().strftime("%Y-%m-%d")
    t_min = t_total / 60.0

    if t_min < 20.0:
        verdict = f"PASS: {t_min:.1f} min — within gate threshold (< 20 min)"
    elif t_min < 25.0:
        verdict = (
            f"CAUTION: {t_min:.1f} min — within budget but approaching"
            " limit (20–25 min)"
        )
    else:
        verdict = (
            f"BLOCKED: {t_min:.1f} min — exceeds 25-min threshold; "
            "open caching-strategy-revisit issue"
        )

    mode_str = f"sample (n={sample_n})" if sample_mode else "full"
    py_ver = sys.version.split()[0]

    content = f"""# Timing Spike — {today}

## Machine Info

- Platform: {platform.machine()}
- OS: {platform.system()}
- Python: {py_ver}

## Run Mode

- Mode: {mode_str}
- Addresses: {address_count:,}
- Stops: {stop_count:,}
- Routing results: {result_count:,}

## Stage Timing

| Stage | Time (s) | Time (min) |
|---|---|---|
| network_build | {t_network:.1f} | {t_network / 60:.2f} |
| address_fetch | {t_addresses:.1f} | {t_addresses / 60:.2f} |
| gtfs_fetch | {t_gtfs:.1f} | {t_gtfs / 60:.2f} |
| routing | {t_routing:.1f} | {t_routing / 60:.2f} |
| stratify_lens | {t_lens:.1f} | {t_lens / 60:.2f} |
| stratify_grid | {t_grid:.1f} | {t_grid / 60:.2f} |
| **Total** | **{t_total:.1f}** | **{t_total / 60:.2f}** |

## Memory

- Peak Python memory (tracemalloc): {peak_mb:.1f} MB
- Note: tracemalloc measures Python allocations only; C extensions (pandana, numpy) \
allocate outside Python's heap.

## Budget Projection

- Total time: {t_min:.2f} min
- Gate threshold: 20 min (within 30-min GHA budget)
- **Verdict: {verdict}**
"""

    _DOCS_DIR.mkdir(parents=True, exist_ok=True)
    doc_path = _DOCS_DIR / f"timing-spike-{today}.md"
    doc_path.write_text(content)
    logger.info("Timing spike document: %s", doc_path)


def _write_lens_verification_doc(
    lens_flags_data: list[dict[str, object]],
) -> None:
    """Write lens verification markdown to pipeline/docs/lens-verification.md."""
    lines = [
        "# Lens Verification — Equity Flag Audit",
        "",
        "| Neighbourhood | analysis_neighborhoods | ej_communities "
        "| equity_strategy | flag_count |",
        "|---|---|---|---|---|",
    ]
    sorted_data = sorted(
        lens_flags_data, key=lambda r: str(r.get("neighborhood_name", ""))
    )
    for row in sorted_data:
        flags: dict[str, object] = row.get("lens_flags", {})  # type: ignore[assignment]
        lines.append(
            f"| {row['neighborhood_name']} "
            f"| {flags.get('analysis_neighborhoods', '')} "
            f"| {flags.get('ej_communities', '')} "
            f"| {flags.get('equity_strategy', '')} "
            f"| {row.get('lens_flag_count', '')} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- EJ Communities filtered to CalEnviroScreen score >= 21 "
            "(top 1/3 of cumulative burden).",
            "- Equity Strategy polygons may not align exactly with Analysis "
            "Neighbourhood boundaries; edge-case addresses can cause a "
            "neighbourhood to inherit an equity flag from an adjacent polygon.",
            "- In sample mode, per-neighbourhood counts are small; a full "
            "run gives more representative flags.",
        ]
    )
    _DOCS_DIR.mkdir(parents=True, exist_ok=True)
    doc_path = _DOCS_DIR / "lens-verification.md"
    doc_path.write_text("\n".join(lines))
    logger.info("Lens verification document: %s", doc_path)


def _run_stratify(
    result: pl.DataFrame,
    stops_df: pl.DataFrame,
    config: Config,
) -> tuple[
    pl.DataFrame,
    list[dict[str, object]],
    list[NeighborhoodGrid],
    CityWide,
    float,
    float,
]:
    """Run stratify_lens and stratify_grid stages.

    Returns (stratified, lens_flags_data, neighborhoods, city_wide,
    t_lens, t_grid).
    """
    logger.info("Stage stratify_lens: starting")
    t0 = time.perf_counter()
    stratified = aggregate_to_lenses(result, stops_df, config)
    lens_flags_data = compute_lens_flags(stratified)
    t_lens = time.perf_counter() - t0
    logger.info("Stage stratify_lens: %.1fs", t_lens)

    logger.info("Stage stratify_grid: starting")
    t0 = time.perf_counter()
    neighborhoods, city_wide = compute_grid(stratified, config)
    t_grid = time.perf_counter() - t0
    logger.info("Stage stratify_grid: %.1fs", t_grid)

    # Cache stratified result for Story 1.10
    buf = io.BytesIO()
    stratified.write_parquet(buf)
    cache = CacheManager(
        root=config.ingest.cache_dir, ttl_days=config.ingest.cache_ttl_days
    )
    spath = cache.put("stratify", "stratified-result", buf.getvalue(), "parquet")
    logger.info("Stratified result cached: %s", spath)

    # Compute default indices for headline number
    freq_idx = config.grid.frequency_threshold_min.index(
        config.grid.defaults.frequency_min
    )
    walk_idx = config.grid.walking_minutes.index(config.grid.defaults.walking_min)
    headline = city_wide.pct_within[freq_idx][walk_idx] if stratified.height else 0.0
    logger.info(
        "Stratify summary: %d neighbourhoods, headline=%.4f",
        len(neighborhoods),
        headline,
    )

    _write_lens_verification_doc(lens_flags_data)

    return stratified, lens_flags_data, neighborhoods, city_wide, t_lens, t_grid


def _check_routing_integrity(result: pl.DataFrame, address_count: int) -> None:
    """Validate routing result integrity and log output statistics."""
    null_dist = result["nearest_stop_distance_m"].null_count()
    if null_dist > 0:
        raise ValueError(
            f"Integrity: {null_dist} null nearest_stop_distance_m value(s)"
        )
    null_stop_id = result["nearest_stop_id"].null_count()
    if null_stop_id > 0:
        logger.warning(
            "%d address(es) have null nearest_stop_id (unreachable)",
            null_stop_id,
        )
    if len(result) != address_count:
        logger.warning(
            "Result rows (%d) != input address rows (%d) — null lat/lon rows dropped",
            len(result),
            address_count,
        )
    logger.info(
        "Output stats: rows=%d mean=%.1fm median=%.1fm max=%.1fm null_stop_id=%d",
        len(result),
        result["nearest_stop_distance_m"].mean() or 0.0,
        result["nearest_stop_distance_m"].median() or 0.0,
        result["nearest_stop_distance_m"].max() or 0.0,
        null_stop_id,
    )


def _print_summary(
    *,
    config: Config,
    address_count: int,
    stop_count: int,
    result_count: int,
    nbhd_count: int,
    city_wide: CityWide,
    t_network: float,
    t_addresses: float,
    t_gtfs: float,
    t_routing: float,
    t_lens: float,
    t_grid: float,
    t_total: float,
    peak_mb: float,
) -> None:
    """Print pipeline summary to stdout."""
    freq_idx = config.grid.frequency_threshold_min.index(
        config.grid.defaults.frequency_min
    )
    walk_idx = config.grid.walking_minutes.index(config.grid.defaults.walking_min)
    headline = city_wide.pct_within[freq_idx][walk_idx]
    sample = config.dev.sample_size
    mode_str = f"sample (n={sample})" if sample is not None else "full"

    print("\n=== Pipeline Summary ===")
    print(f"Mode:         {mode_str}")
    print(f"Addresses:    {address_count:,}")
    print(f"Stops:        {stop_count:,}")
    print(f"Results:      {result_count:,}")
    print(f"Neighbourhoods: {nbhd_count}")
    print(f"Headline:     {headline:.4f}")
    print("\nStage timing:")
    print(f"  network_build   {t_network:8.1f}s")
    print(f"  address_fetch   {t_addresses:8.1f}s")
    print(f"  gtfs_fetch      {t_gtfs:8.1f}s")
    print(f"  routing         {t_routing:8.1f}s")
    print(f"  stratify_lens   {t_lens:8.1f}s")
    print(f"  stratify_grid   {t_grid:8.1f}s")
    print(f"  TOTAL           {t_total:8.1f}s  ({t_total / 60:.1f} min)")
    print(f"\nPeak Python memory: {peak_mb:.1f} MB")


def _run_pipeline(config: Config) -> None:
    """Execute all pipeline stages with timing, memory, and caching."""
    tracemalloc.start()
    t_start = time.perf_counter()

    t0 = time.perf_counter()
    net, _osm_date = build_network(config)
    t_network = time.perf_counter() - t0
    logger.info("Stage network_build: %.1fs", t_network)

    t0 = time.perf_counter()
    addresses = fetch_residential_addresses(config)
    t_addresses = time.perf_counter() - t0
    logger.info("Stage address_fetch: %.1fs", t_addresses)

    t0 = time.perf_counter()
    stops_df, _gtfs_sha256 = fetch_gtfs(config)
    t_gtfs = time.perf_counter() - t0
    logger.info("Stage gtfs_fetch: %.1fs", t_gtfs)

    t0 = time.perf_counter()
    result = route_nearest_stops(net, addresses, stops_df, config)
    t_routing = time.perf_counter() - t0
    logger.info("Stage routing: %.1fs", t_routing)

    _cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / 1_048_576.0
    if peak_mb / 1024.0 > 6.0:
        logger.warning("Peak memory %.1f MB exceeds 6 GB threshold", peak_mb)
    else:
        logger.info("Peak memory: %.1f MB", peak_mb)
    _check_routing_integrity(result, len(addresses))

    # Cache routing result
    buf = io.BytesIO()
    result.write_parquet(buf)
    cache = CacheManager(
        root=config.ingest.cache_dir, ttl_days=config.ingest.cache_ttl_days
    )
    cache.put("routing", "routing-result", buf.getvalue(), "parquet")

    # Stratify stages (lens + grid)
    _strat, _flags, neighborhoods, city_wide, t_lens, t_grid = _run_stratify(
        result, stops_df, config
    )
    t_total = time.perf_counter() - t_start
    logger.info("Total pipeline time: %.1fs", t_total)

    _write_timing_doc(
        t_network=t_network,
        t_addresses=t_addresses,
        t_gtfs=t_gtfs,
        t_routing=t_routing,
        t_lens=t_lens,
        t_grid=t_grid,
        t_total=t_total,
        peak_mb=peak_mb,
        address_count=len(addresses),
        stop_count=len(stops_df),
        result_count=len(result),
        sample_mode=config.dev.sample_size is not None,
        sample_n=config.dev.sample_size,
    )

    _print_summary(
        config=config,
        address_count=len(addresses),
        stop_count=len(stops_df),
        result_count=len(result),
        nbhd_count=len(neighborhoods),
        city_wide=city_wide,
        t_network=t_network,
        t_addresses=t_addresses,
        t_gtfs=t_gtfs,
        t_routing=t_routing,
        t_lens=t_lens,
        t_grid=t_grid,
        t_total=t_total,
        peak_mb=peak_mb,
    )


def main() -> None:
    """Run the muni-walk-access pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="muni-walk-access",
        description=(
            "SF MUNI walkshed accessibility pipeline — computes "
            "transit access scores for all SF residential addresses."
        ),
    )
    parser.add_argument(
        "--sample",
        type=int,
        metavar="N",
        default=None,
        help="Run in sample mode with N addresses (default: full dataset ~200k).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        metavar="PATH",
        help="Path to config.yaml (default: config.yaml).",
    )
    args = parser.parse_args()

    if args.sample is not None and args.sample <= 0:
        parser.error("--sample must be a positive integer")

    try:
        config = load_config(Path(args.config))

        if args.sample is not None:
            config = config.model_copy(
                update={
                    "dev": config.dev.model_copy(update={"sample_size": args.sample})
                }
            )

        _run_pipeline(config)

    except ValidationError as exc:
        print(f"Config validation failed:\n{exc}", file=sys.stderr)
        sys.exit(1)
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as exc:
        print(f"Config YAML syntax error:\n{exc}", file=sys.stderr)
        sys.exit(1)
    except IngestError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except NetworkBuildError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except Exception:
        logger.error("Pipeline failed with unexpected error", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
