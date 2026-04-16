"""Equity-lens spatial join and flag computation.

Assigns each routed address to its Analysis Neighborhood, Environmental
Justice Community membership, and SFMTA Equity Strategy membership via
spatial join against DataSF boundary datasets.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import geopandas as gpd
import httpx
import pandas as pd
import polars as pl

from muni_walk_access.config import Config
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import SODA_BASE

logger = logging.getLogger(__name__)

# Column in the Analysis Neighborhoods GeoJSON that holds the name.
_ANA_NAME_COL = "nhood"

# EJ Communities score threshold: top 1/3 of cumulative burden (scores 21-30).
_EJ_SCORE_THRESHOLD = 21


def slugify_neighborhood(name: str) -> str:
    """Convert a neighbourhood name to a kebab-case slug.

    This is the single source of truth for neighbourhood ID generation.
    """
    s = name.lower()
    s = s.replace("'", "")  # strip apostrophes before slugifying
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s


def _fetch_lens_geojson(
    dataset_id: str,
    config: Config,
) -> Path:
    """Fetch a lens boundary GeoJSON, raising the SODA row limit.

    ``fetch_geospatial`` uses the SODA default of 1 000 rows, which
    truncates the EJ Communities dataset (2 700+ tracts).  This helper
    requests up to 50 000 rows so the full dataset is cached.
    """
    cache = CacheManager(
        root=config.ingest.cache_dir,
        ttl_days=config.ingest.cache_ttl_days,
    )
    fresh = cache.get("datasf", dataset_id)
    if fresh is not None:
        return fresh
    url = f"{SODA_BASE}/{dataset_id}.geojson?$limit=50000"
    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except (httpx.HTTPError, httpx.TransportError) as exc:
        stale = cache.get_any("datasf", dataset_id)
        if stale is not None:
            logger.warning(
                "Lens boundary fetch failed for %s (%s); returning stale cache %s",
                dataset_id,
                exc,
                stale,
            )
            return stale
        raise IngestError(
            dataset_id,
            f"HTTP error and no local cache: {exc}. "
            "Warm the cache with network access first.",
        ) from exc
    path = cache.put("datasf", dataset_id, resp.content, "geojson")
    logger.info("Lens boundary fetched: %s (%d bytes)", path, len(resp.content))
    return path


def _fetch_boundaries(config: Config) -> dict[str, gpd.GeoDataFrame]:
    """Fetch all equity-lens boundary GeoDataFrames from DataSF cache."""
    boundaries: dict[str, gpd.GeoDataFrame] = {}
    for lens in config.lenses:
        path = _fetch_lens_geojson(lens.datasf_id, config)
        gdf: gpd.GeoDataFrame = gpd.read_file(path)
        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        # EJ Communities is a scoring dataset covering all tracts;
        # filter to score >= 21 (top 1/3 = EJ designation).
        if lens.id == "ej_communities":
            if "score" not in gdf.columns:
                logger.warning(
                    "EJ Communities dataset missing 'score' column "
                    "(columns: %s); treating ALL tracts as EJ — "
                    "results may be incorrect",
                    list(gdf.columns),
                )
            else:
                gdf["score"] = pd.to_numeric(gdf["score"], errors="coerce")
                before = len(gdf)
                gdf = gdf[gdf["score"] >= _EJ_SCORE_THRESHOLD]
                logger.info(
                    "EJ Communities: %d/%d tracts meet score >= %d",
                    len(gdf),
                    before,
                    _EJ_SCORE_THRESHOLD,
                )
        boundaries[lens.id] = gdf
    return boundaries


def _sjoin_boolean(
    addr_gdf: gpd.GeoDataFrame,
    boundary: gpd.GeoDataFrame,
) -> list[bool]:
    """Return per-address boolean: True if address falls within any polygon."""
    hit = gpd.sjoin(
        addr_gdf[["geometry"]],
        boundary[["geometry"]],
        how="inner",
        predicate="within",
    )
    hit_indices: set[int] = set(hit.index)
    return [i in hit_indices for i in addr_gdf.index]


def aggregate_to_lenses(
    routing_result: pl.DataFrame,
    stops_df: pl.DataFrame,
    config: Config,
    time_window: str | None = None,
) -> pl.DataFrame:
    """Spatially join routed addresses to equity-lens boundaries.

    Args:
        routing_result: Routed address DataFrame with nearest_stop_id.
        stops_df: Stop frequency data. For v2 (multi-window), this is the
            summary table with a ``time_window`` column — pass ``time_window``
            to filter before joining.  For v1 (legacy), this has
            ``trips_per_hour_peak`` and no time_window column.
        config: Pipeline configuration.
        time_window: If set, filter stops_df to this window before joining.
            Required when stops_df has multiple rows per stop_id.

    Returns a Polars DataFrame with the original routing columns plus:
    ``neighborhood_id``, ``neighborhood_name``, ``ej_community``,
    ``equity_strategy``, ``trips_per_hour_peak``.

    Addresses that fall outside all three boundaries are excluded with
    a log INFO message.

    """
    if len(routing_result) == 0:
        return routing_result.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("neighborhood_id"),
            pl.lit(None).cast(pl.Utf8).alias("neighborhood_name"),
            pl.lit(None).cast(pl.Boolean).alias("ej_community"),
            pl.lit(None).cast(pl.Boolean).alias("equity_strategy"),
            pl.lit(None).cast(pl.Float64).alias("trips_per_hour_peak"),
        )

    boundaries = _fetch_boundaries(config)

    # Convert to GeoDataFrame
    addr_pd = routing_result.to_pandas()
    addr_gdf = gpd.GeoDataFrame(
        addr_pd,
        geometry=gpd.points_from_xy(addr_pd["longitude"], addr_pd["latitude"]),
        crs="EPSG:4326",
    )

    # --- Analysis Neighbourhoods: get name per address ---
    ana_bnd = boundaries["analysis_neighborhoods"]
    # Rename to avoid collision with EAS 'nhood' column already in routing result
    result_col = "neighborhood_name"
    ana_slim = ana_bnd[[_ANA_NAME_COL, "geometry"]].rename(
        columns={_ANA_NAME_COL: result_col}
    )
    ana_joined = gpd.sjoin(addr_gdf, ana_slim, how="left", predicate="within")
    ana_joined = ana_joined[~ana_joined.index.duplicated(keep="first")]
    neighbourhood_names = ana_joined[result_col]

    # --- EJ Communities: boolean per address ---
    ej_flags = _sjoin_boolean(addr_gdf, boundaries["ej_communities"])

    # --- Equity Strategy: boolean per address ---
    eq_flags = _sjoin_boolean(addr_gdf, boundaries["equity_strategy"])

    # Assemble result on the original pandas frame
    result_pd = addr_pd.copy()
    result_pd["neighborhood_name"] = neighbourhood_names.values
    result_pd["ej_community"] = ej_flags
    result_pd["equity_strategy"] = eq_flags

    # Exclude addresses outside all boundaries (no neighbourhood match)
    outside_mask = result_pd["neighborhood_name"].isna()
    outside_count = int(outside_mask.sum())
    if outside_count > 0:
        logger.info(
            "%d address(es) fall outside all boundaries; "
            "excluding from equity aggregation",
            outside_count,
        )
        result_pd = result_pd[~outside_mask]

    # Generate slug ID
    result_pd["neighborhood_id"] = result_pd["neighborhood_name"].apply(
        slugify_neighborhood
    )

    # Prepare stop frequency data for join.
    # v2 summary: stop_id, time_window, total_trips_per_hour, ...
    # v1 legacy: stop_id, trips_per_hour_peak, ...
    if "time_window" in stops_df.columns and time_window is not None:
        # v2 path: filter to requested window, rename for downstream compat
        window_stops = stops_df.filter(pl.col("time_window") == time_window)
        stops_pd = window_stops.select(
            [
                "stop_id",
                pl.col("total_trips_per_hour").alias("trips_per_hour_peak"),
            ]
        ).to_pandas()
    else:
        # v1 legacy path
        stops_pd = stops_df.select(["stop_id", "trips_per_hour_peak"]).to_pandas()

    result_pd = result_pd.merge(
        stops_pd,
        left_on="nearest_stop_id",
        right_on="stop_id",
        how="left",
    )
    # Drop the extra stop_id column from the merge
    if "stop_id" in result_pd.columns:
        result_pd = result_pd.drop(columns=["stop_id"])

    # Build final column list
    orig_cols = list(routing_result.columns)
    new_cols = [
        "neighborhood_id",
        "neighborhood_name",
        "ej_community",
        "equity_strategy",
        "trips_per_hour_peak",
    ]
    keep = [c for c in orig_cols + new_cols if c in result_pd.columns]
    return pl.from_pandas(result_pd[keep])


def restratify_for_window(
    stratified: pl.DataFrame,
    summary_df: pl.DataFrame,
    time_window: str,
) -> pl.DataFrame:
    """Swap trips_per_hour_peak for a different time window.

    Avoids re-running spatial joins.

    Takes an already-stratified DataFrame (from ``aggregate_to_lenses``) and
    replaces the ``trips_per_hour_peak`` column with values from a different
    time window in the summary table. Much cheaper than re-running the full
    spatial join pipeline.
    """
    window_stops = summary_df.filter(pl.col("time_window") == time_window)
    freq_lookup = window_stops.select(
        [
            "stop_id",
            pl.col("total_trips_per_hour").alias("_new_tph"),
        ]
    )

    result = (
        stratified.drop("trips_per_hour_peak")
        .join(
            freq_lookup,
            left_on="nearest_stop_id",
            right_on="stop_id",
            how="left",
        )
        .rename({"_new_tph": "trips_per_hour_peak"})
    )

    return result


def compute_lens_flags(
    stratified: pl.DataFrame,
) -> list[dict[str, Any]]:
    """Compute per-neighbourhood equity-lens flags.

    Returns a list of dicts sorted by ``neighborhood_id`` with keys:
    ``neighborhood_id``, ``neighborhood_name``, ``lens_flags`` (dict),
    ``lens_flag_count`` (int 0-3).
    """
    if len(stratified) == 0:
        return []

    grouped = stratified.group_by("neighborhood_id").agg(
        pl.first("neighborhood_name"),
        pl.col("ej_community").any().alias("ej_communities"),
        pl.col("equity_strategy").any().alias("equity_strategy_flag"),
    )

    results: list[dict[str, Any]] = []
    for row in grouped.iter_rows(named=True):
        flags = {
            "analysis_neighborhoods": True,
            "ej_communities": bool(row["ej_communities"]),
            "equity_strategy": bool(row["equity_strategy_flag"]),
        }
        results.append(
            {
                "neighborhood_id": row["neighborhood_id"],
                "neighborhood_name": row["neighborhood_name"],
                "lens_flags": flags,
                "lens_flag_count": sum(flags.values()),
            }
        )

    results.sort(key=lambda r: str(r["neighborhood_id"]))
    return results
