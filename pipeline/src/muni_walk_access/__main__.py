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
from muni_walk_access.ingest.gtfs import (
    compute_frequencies,
    fetch_gtfs,
    fetch_gtfs_feed,
)
from muni_walk_access.ingest.sources import get_address_source
from muni_walk_access.ingest.sources.datasf import fetch_datasf_metadata
from muni_walk_access.network.build import build_network
from muni_walk_access.route.nearest_stop import route_nearest_stops
from muni_walk_access.run_context import RunContext
from muni_walk_access.stratify.grid import (
    assign_hex_cells,
    compute_grid,
    compute_hex_grids,
)
from muni_walk_access.stratify.lens import (
    aggregate_to_lenses,
    compute_lens_flags,
    restratify_for_window,
)

logger = logging.getLogger(__name__)


def _run_stratify(
    result: pl.DataFrame,
    stops_df: pl.DataFrame,
    config: Config,
    time_window: str | None = None,
    ctx: RunContext | None = None,
) -> tuple[
    pl.DataFrame,
    list[dict[str, object]],
    list[NeighborhoodGrid],
    CityWide,
    float,
    float,
]:
    """Run stratify_lens and stratify_grid stages.

    Args:
        result: Routed address DataFrame.
        stops_df: Stop frequency DataFrame.
        config: Pipeline configuration.
        time_window: If set, filter stops_df to this window before joining.
        ctx: Run context for provenance (records lens-boundary dataset
            timestamps into ``ctx.datasf_timestamps`` via
            :class:`DataSFBoundarySource`).

    Returns (stratified, lens_flags_data, neighborhoods, city_wide,
    t_lens, t_grid).

    """
    tw_label = f" [{time_window}]" if time_window else ""
    logger.info("Stage stratify_lens%s: starting", tw_label)
    t0 = time.perf_counter()
    stratified = aggregate_to_lenses(
        result, stops_df, config, time_window=time_window, ctx=ctx
    )
    lens_flags_data = compute_lens_flags(stratified, config)
    t_lens = time.perf_counter() - t0
    logger.info("Stage stratify_lens%s: %.1fs", tw_label, t_lens)

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
    """Validate routing result integrity and log output statistics.

    Unreachable addresses (no stop within config.routing.max_distance_m) are
    expected to have null distance/stop_id — they are logged, not an error.
    An abnormally high null ratio signals a data problem (wrong CRS, bad stop
    coordinates, OSM network gap) and is surfaced as a WARNING.
    """
    null_dist = result["nearest_stop_distance_m"].null_count()
    null_stop_id = result["nearest_stop_id"].null_count()
    if null_dist != null_stop_id:
        raise ValueError(
            f"Integrity: null distance count ({null_dist}) != "
            f"null stop_id count ({null_stop_id}) — unreachable rows should "
            "have both columns null"
        )
    if null_dist > 0:
        pct = 100.0 * null_dist / max(len(result), 1)
        if pct > 5.0:
            logger.warning(
                "%d/%d addresses unreachable (%.2f%%) — likely data issue",
                null_dist,
                len(result),
                pct,
            )
        else:
            logger.info(
                "%d/%d addresses unreachable (%.2f%%)",
                null_dist,
                len(result),
                pct,
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
    gtfs_feed_date: str,
    osm_date: str,
    datasf_timestamps: dict[str, str],
    datasf_data_updated: dict[str, str],
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
        gtfs_feed_date=gtfs_feed_date,
        osm_date=osm_date,
        datasf_timestamps=datasf_timestamps,
        datasf_data_updated=datasf_data_updated,
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
    output_dir: Path,
) -> None:
    """Execute all pipeline stages with timing, memory, and caching."""
    run_id = datetime.now(timezone.utc).isoformat()
    config_hash, git_sha, git_tag = _get_git_provenance(config_path)

    cache = CacheManager(
        root=config.ingest.cache_dir, ttl_days=config.ingest.cache_ttl_days
    )
    ctx = RunContext.from_config(run_id=run_id, config=config, cache=cache)
    logger.info("RunContext built (city_id=%s)", ctx.city_id)

    tracemalloc.start()
    t_start = time.perf_counter()

    t0 = time.perf_counter()
    net, osm_date = build_network(config, ctx=ctx)
    t_network = time.perf_counter() - t0
    logger.info("Stage network_build: %.1fs", t_network)

    t0 = time.perf_counter()
    address_source = get_address_source(config.address_source.kind)()
    addresses = address_source.fetch(ctx)
    t_addresses = time.perf_counter() - t0
    logger.info("Stage address_fetch: %.1fs", t_addresses)

    time_windows = config.frequency.time_windows
    use_v2 = bool(time_windows)

    t0 = time.perf_counter()
    gtfs_feed_date = ""
    if use_v2:
        gtfs_feed = fetch_gtfs_feed(config, ctx=ctx)
        detail_df, summary_df = compute_frequencies(gtfs_feed, config)
        gtfs_sha256 = gtfs_feed.feed_sha256
        gtfs_feed_date = gtfs_feed.feed_date
        # For routing, we need a single stops DataFrame with coordinates.
        # Use the summary filtered to am_peak (or first window) for routing —
        # routing only needs stop_id, stop_lat, stop_lon, trips_per_hour_peak.
        default_window = time_windows[0].key
        routing_stops = (
            summary_df.filter(pl.col("time_window") == default_window)
            .rename({"total_trips_per_hour": "trips_per_hour_peak"})
            .select(["stop_id", "trips_per_hour_peak", "stop_lat", "stop_lon"])
        )
        stop_count = routing_stops["stop_id"].n_unique()
    else:
        routing_stops, gtfs_sha256, gtfs_feed_date = fetch_gtfs(config, ctx=ctx)
        stop_count = len(routing_stops)
    t_gtfs = time.perf_counter() - t0
    logger.info("Stage gtfs_fetch: %.1fs", t_gtfs)

    t0 = time.perf_counter()
    result = route_nearest_stops(net, addresses, routing_stops, config)
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

    if use_v2:
        # --- Multi-window path ---
        # Stratify once with default window for the spatial join base
        stratified, _flags, neighborhoods, city_wide, t_lens, t_grid = _run_stratify(
            result, summary_df, config, time_window=default_window, ctx=ctx
        )

        # Per-window grid + hex: loop once per time window (aggregate + headway)
        logger.info("Stage stratify_hex: starting (%d windows)", len(time_windows))
        t0 = time.perf_counter()
        all_hex_grids: dict[str, dict[int, list[HexCell]]] = {}
        per_window_grids: dict[str, tuple[list[NeighborhoodGrid], CityWide]] = {}
        all_hex_grids_headway: dict[str, dict[int, list[HexCell]]] = {}
        per_window_grids_headway: dict[
            str, tuple[list[NeighborhoodGrid], CityWide]
        ] = {}
        for tw in time_windows:
            if tw.key == default_window:
                tw_stratified = stratified  # already computed above
                per_window_grids[tw.key] = (neighborhoods, city_wide)
            else:
                tw_stratified = restratify_for_window(stratified, summary_df, tw.key)
                tw_neighborhoods, tw_city_wide = compute_grid(tw_stratified, config)
                per_window_grids[tw.key] = (tw_neighborhoods, tw_city_wide)
            # Pre-assign H3 cells once — reused by both aggregate and headway
            tw_with_hex = assign_hex_cells(tw_stratified)
            # Aggregate scoring (existing)
            tw_hex = compute_hex_grids(tw_with_hex, config)
            all_hex_grids[tw.key] = tw_hex
            # Headway scoring (single-route wait time)
            if "best_route_headway_min" in tw_stratified.columns:
                hw_neighborhoods, hw_city_wide = compute_grid(
                    tw_stratified, config, metric="headway"
                )
                per_window_grids_headway[tw.key] = (hw_neighborhoods, hw_city_wide)
                hw_hex = compute_hex_grids(tw_with_hex, config, metric="headway")
                all_hex_grids_headway[tw.key] = hw_hex
            logger.info(
                "  Hex grids [%s]: %s",
                tw.key,
                {r: len(cells) for r, cells in tw_hex.items()},
            )
        t_hex = time.perf_counter() - t0
        has_headway = bool(all_hex_grids_headway)
        logger.info(
            "Stage stratify_hex: %.1fs (%d windows × %s)",
            t_hex,
            len(time_windows),
            "aggregate+headway" if has_headway else "aggregate",
        )
    else:
        # --- Legacy single-window path ---
        stratified, _flags, neighborhoods, city_wide, t_lens, t_grid = _run_stratify(
            result, routing_stops, config, ctx=ctx
        )
        logger.info("Stage stratify_hex: starting")
        t0 = time.perf_counter()
        all_hex_grids = {"_legacy": compute_hex_grids(stratified, config)}
        t_hex = time.perf_counter() - t0
        logger.info("Stage stratify_hex: %.1fs", t_hex)

    # Collect provenance after stratify (boundary datasets are fetched there)
    datasf_timestamps = dict(ctx.datasf_timestamps)
    upstream_fallback = ctx.upstream_fallback

    if skip_validation:
        logger.info("Validation skipped (--skip-validation)")

    # Emit stage — output_dir is passed in from main() (--output-dir flag).
    # For v2, emit per-window hex files; for legacy, emit without time_window
    hex_grids_for_emit: dict[int, list[HexCell]] = {}
    if use_v2:
        # Write per-window hex + grid JSON files (aggregate)
        for tw_key, tw_hex in all_hex_grids.items():
            if tw_hex:
                write_grid_hex_json(
                    tw_hex, config, run_id, output_dir, time_window=tw_key
                )
        for tw_key, (tw_nbhds, tw_cw) in per_window_grids.items():
            write_grid_json(
                tw_nbhds, tw_cw, config, run_id, output_dir, time_window=tw_key
            )
        # Write per-window hex + grid JSON files (headway)
        for tw_key, tw_hex in all_hex_grids_headway.items():
            if tw_hex:
                write_grid_hex_json(
                    tw_hex,
                    config,
                    run_id,
                    output_dir,
                    time_window=tw_key,
                    route_mode="headway",
                )
        for tw_key, (tw_nbhds, tw_cw) in per_window_grids_headway.items():
            write_grid_json(
                tw_nbhds,
                tw_cw,
                config,
                run_id,
                output_dir,
                time_window=tw_key,
                route_mode="headway",
            )
    else:
        hex_grids_for_emit = all_hex_grids.get("_legacy", {})

    datasf_data_updated = fetch_datasf_metadata(list(datasf_timestamps.keys()))

    t_emit = _run_emit(
        neighborhoods,
        city_wide,
        hex_grids_for_emit if not use_v2 else {},
        stratified,
        config,
        run_id,
        git_sha,
        git_tag,
        config_hash,
        gtfs_sha256,
        gtfs_feed_date,
        osm_date,
        datasf_timestamps,
        datasf_data_updated,
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
        stop_count=stop_count,
        result_count=len(result),
        sample_mode=config.dev.sample_size is not None,
        sample_n=config.dev.sample_size,
    )

    _print_summary(
        config=config,
        address_count=len(addresses),
        stop_count=stop_count,
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
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        metavar="PATH",
        help=(
            "Root directory where emit writes site/src/data/* and "
            "site/public/{data,downloads}/*. Default: the repo root "
            "(three levels above pipeline/)."
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

        output_dir = (
            Path(args.output_dir).resolve()
            if args.output_dir is not None
            else Path(__file__).resolve().parent.parent.parent.parent
        )
        _run_pipeline(
            config,
            config_path=config_path,
            skip_validation=args.skip_validation,
            output_dir=output_dir,
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
