"""Entry point for the muni-walk-access pipeline."""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import subprocess
import sys
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import yaml
from pydantic import ValidationError

from muni_walk_access.config import Config, load_config
from muni_walk_access.emit.config_snapshot import write_config_snapshot
from muni_walk_access.emit.docs import _write_lens_verification_doc, _write_timing_doc
from muni_walk_access.emit.downloads import write_downloads
from muni_walk_access.emit.geojson import write_neighborhoods_geojson
from muni_walk_access.emit.grid_hex_json import write_grid_hex_json
from muni_walk_access.emit.grid_json import write_grid_json
from muni_walk_access.emit.schemas import CityWide, HexCell, NeighborhoodGrid
from muni_walk_access.exceptions import IngestError, NetworkBuildError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import (
    fetch_residential_addresses,
    get_datasf_timestamps,
    was_fallback_used,
)
from muni_walk_access.ingest.gtfs import fetch_gtfs
from muni_walk_access.network.build import build_network
from muni_walk_access.route.nearest_stop import route_nearest_stops
from muni_walk_access.stratify.grid import compute_grid, compute_hex_grids
from muni_walk_access.stratify.lens import aggregate_to_lenses, compute_lens_flags

logger = logging.getLogger(__name__)


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
    t_hex: float,
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
    print(f"  stratify_hex    {t_hex:8.1f}s")
    print(f"  TOTAL           {t_total:8.1f}s  ({t_total / 60:.1f} min)")
    print(f"\nPeak Python memory: {peak_mb:.1f} MB")


def _get_git_provenance(config_path: Path) -> tuple[str, str, str]:
    """Return (config_hash, git_sha, git_tag) for run provenance."""
    config_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()
    try:
        git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    except Exception:
        git_sha = "unknown"
    try:
        git_tag = (
            subprocess.check_output(["git", "describe", "--tags", "--always"])
            .decode()
            .strip()
        )
    except Exception:
        git_tag = "unknown"
    return config_hash, git_sha, git_tag


def _run_emit(
    neighborhoods: list[NeighborhoodGrid],
    city_wide: CityWide,
    hex_grids: dict[int, list[HexCell]],
    stratified: pl.DataFrame,
    config: Config,
    run_id: str,
    git_sha: str,
    git_tag: str,
    config_hash: str,
    gtfs_sha256: str,
    osm_date: str,
    datasf_timestamps: dict[str, str],
    upstream_fallback: bool,
    output_dir: Path,
) -> float:
    """Run emit stages: grid_json, grid_hex_json, config_snapshot, geojson, downloads.

    Returns elapsed time in seconds.
    """
    logger.info("Stage emit: starting")
    t0 = time.perf_counter()

    write_grid_json(neighborhoods, city_wide, config, run_id, output_dir)
    if hex_grids:
        write_grid_hex_json(hex_grids, config, run_id, output_dir)
    config_snapshot_path = write_config_snapshot(
        run_id=run_id,
        git_sha=git_sha,
        git_tag=git_tag,
        config_hash=config_hash,
        gtfs_sha256=gtfs_sha256,
        osm_date=osm_date,
        datasf_timestamps=datasf_timestamps,
        upstream_fallback=upstream_fallback,
        config_values=config.model_dump(mode="json"),
        output_dir=output_dir,
    )
    geojson_path = write_neighborhoods_geojson(neighborhoods, config, output_dir)
    write_downloads(
        neighborhoods,
        stratified,
        config_snapshot_path,
        geojson_path,
        run_id,
        output_dir,
        config,
    )

    t_emit = time.perf_counter() - t0
    logger.info("Stage emit: %.1fs", t_emit)
    return t_emit


def _run_pipeline(
    config: Config,
    config_path: Path,
    skip_validation: bool,
) -> None:
    """Execute all pipeline stages with timing, memory, and caching."""
    run_id = datetime.now(timezone.utc).isoformat()
    config_hash, git_sha, git_tag = _get_git_provenance(config_path)

    tracemalloc.start()
    t_start = time.perf_counter()

    t0 = time.perf_counter()
    net, osm_date = build_network(config)
    t_network = time.perf_counter() - t0
    logger.info("Stage network_build: %.1fs", t_network)

    t0 = time.perf_counter()
    addresses = fetch_residential_addresses(config)
    t_addresses = time.perf_counter() - t0
    logger.info("Stage address_fetch: %.1fs", t_addresses)

    t0 = time.perf_counter()
    stops_df, gtfs_sha256 = fetch_gtfs(config)
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
    stratified, _flags, neighborhoods, city_wide, t_lens, t_grid = _run_stratify(
        result, stops_df, config
    )

    # Hex grids (resolution picker, resolutions 7-11)
    logger.info("Stage stratify_hex: starting")
    t0 = time.perf_counter()
    hex_grids = compute_hex_grids(stratified, config)
    t_hex = time.perf_counter() - t0
    logger.info("Stage stratify_hex: %.1fs", t_hex)

    # Collect provenance after stratify (boundary datasets are fetched there)
    datasf_timestamps = get_datasf_timestamps()
    upstream_fallback = was_fallback_used()

    if skip_validation:
        logger.info("Validation skipped (--skip-validation)")

    # Emit stage
    output_dir = Path(__file__).parent.parent.parent.parent
    t_emit = _run_emit(
        neighborhoods,
        city_wide,
        hex_grids,
        stratified,
        config,
        run_id,
        git_sha,
        git_tag,
        config_hash,
        gtfs_sha256,
        osm_date,
        datasf_timestamps,
        upstream_fallback,
        output_dir,
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
        t_hex=t_hex,
        t_emit=t_emit,
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
        t_hex=t_hex,
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
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        default=False,
        help=(
            "Skip the validation gate and proceed directly to emit. "
            "Use before ground-truth fixture is available."
        ),
    )
    args = parser.parse_args()

    if args.sample is not None and args.sample <= 0:
        parser.error("--sample must be a positive integer")

    try:
        config_path = Path(args.config)
        config = load_config(config_path)

        if args.sample is not None:
            config = config.model_copy(
                update={
                    "dev": config.dev.model_copy(update={"sample_size": args.sample})
                }
            )

        _run_pipeline(
            config, config_path=config_path, skip_validation=args.skip_validation
        )

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
