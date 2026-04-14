"""Pandana bulk nearest-stop routing for residential addresses."""

from __future__ import annotations

import logging

import numpy as np
import pandana
import pandas as pd
import polars as pl

from muni_walk_access.config import Config

logger = logging.getLogger(__name__)

_METERS_PER_MILE: float = 1609.34
_MAX_DIST_METERS: float = 5000.0  # 5 km search radius (generous for SF)
_POI_CATEGORY: str = "muni_stops"


def route_nearest_stops(
    net: pandana.Network,
    addresses: pl.DataFrame,
    stops: pl.DataFrame,
    config: Config,
) -> pl.DataFrame:
    """Bulk-compute nearest Muni stop for each residential address via pandana.

    Uses pandana's two-step POI workflow:
      1. Register Muni stops as POIs on the network.
      2. Bulk-compute nearest POI for ALL network nodes (O(nodes), not O(addresses)).
      3. Snap each address to its nearest network node and look up the result.

    Args:
        net: Pre-built pandana pedestrian Network (from network.build).
        addresses: Residential address DataFrame with ``latitude`` and
            ``longitude`` columns (may be Utf8 or Float64).
        stops: GTFS stops DataFrame with ``stop_id``, ``stop_lat``, ``stop_lon``
            columns (from ingest.gtfs.fetch_gtfs).
        config: Root pipeline Config; uses ``config.walking.pace_min_per_mile``
            and ``config.dev.sample_size``.

    Returns:
        DataFrame with all input address columns plus:
          - ``nearest_stop_distance_m`` (Float64): network distance in meters
          - ``walk_minutes`` (Float64): estimated walk time
          - ``nearest_stop_id`` (Utf8 | null): stop_id of the nearest stop,
            or null if no stop is reachable within the search radius

    """
    # --- Early return on empty inputs ---
    if addresses.is_empty():
        logger.warning("No addresses to route — returning empty DataFrame")
        return addresses.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("nearest_stop_distance_m"),
            pl.lit(None, dtype=pl.Float64).alias("walk_minutes"),
            pl.lit(None, dtype=pl.Utf8).alias("nearest_stop_id"),
        )
    if stops.is_empty():
        logger.warning("No stops provided — returning null routing columns")
        return addresses.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("nearest_stop_distance_m"),
            pl.lit(None, dtype=pl.Float64).alias("walk_minutes"),
            pl.lit(None, dtype=pl.Utf8).alias("nearest_stop_id"),
        )

    # --- Sample mode (T4) ---
    sample_size = config.dev.sample_size
    if sample_size is not None:
        sample_size = min(sample_size, len(addresses))
        logger.info(
            "Sample mode: routing %d of %d addresses (seed=42)",
            sample_size,
            len(addresses),
        )
        addresses = addresses.sample(n=sample_size, seed=42)
    else:
        logger.info("Full mode: routing all %d addresses", len(addresses))

    # --- Cast lat/lon to float (T5) ---
    # fetch_tabular returns all-String columns; cast only if needed to avoid
    # double-casting already-numeric data.
    if addresses["latitude"].dtype == pl.Utf8:
        addresses = addresses.with_columns(
            pl.col("latitude").cast(pl.Float64),
            pl.col("longitude").cast(pl.Float64),
        )

    # Drop rows with null lat/lon (malformed EAS data) to avoid silent bad routing.
    pre_count = len(addresses)
    addresses = addresses.filter(
        pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null()
    )
    if len(addresses) < pre_count:
        logger.warning(
            "Dropped %d address(es) with null lat/lon after cast",
            pre_count - len(addresses),
        )

    # Stop coordinates: fetch_gtfs returns typed floats, but cast defensively.
    if stops["stop_lat"].dtype == pl.Utf8:
        stops = stops.with_columns(
            pl.col("stop_lat").cast(pl.Float64),
            pl.col("stop_lon").cast(pl.Float64),
        )

    # Build ordered stop arrays for POI registration.
    # The index order here determines the poi1 index returned by nearest_pois.
    # Use .to_list() + pd.Series() to avoid a pyarrow dependency from .to_pandas().
    stop_lons: pd.Series = pd.Series(stops["stop_lon"].to_list())
    stop_lats: pd.Series = pd.Series(stops["stop_lat"].to_list())
    stop_ids: list[str] = stops["stop_id"].to_list()

    # --- Register Muni stops as POIs (T3b) ---
    net.set_pois(
        category=_POI_CATEGORY,
        maxdist=_MAX_DIST_METERS,
        maxitems=1,
        x_col=stop_lons,  # longitude = x
        y_col=stop_lats,  # latitude = y
    )

    # --- Bulk compute nearest POI for all network nodes (T3c) ---
    distances = net.nearest_pois(
        distance=_MAX_DIST_METERS,
        category=_POI_CATEGORY,
        num_pois=1,
        include_poi_ids=True,
    )
    # distances: pandas DataFrame indexed by node_id
    #   column 1    → distance to nearest stop (meters)
    #   column "poi1" → 0-based index into the stop arrays passed to set_pois

    # --- Snap addresses to nearest network nodes (T3d) ---
    addr_lons = addresses["longitude"].to_numpy()
    addr_lats = addresses["latitude"].to_numpy()
    node_ids = net.get_node_ids(addr_lons, addr_lats)  # lon=x, lat=y

    # Look up pre-computed results for each address's nearest node.
    # When no stop is within maxdist, pandana returns NaN for poi1.
    network_distances = distances.loc[node_ids, 1].to_numpy()
    raw_poi_idx = distances.loc[node_ids, "poi1"].to_numpy()
    # Track which addresses are unreachable (NaN poi index).
    reachable = ~np.isnan(raw_poi_idx)
    # Safe int cast: fill NaN with 0 for indexing only; unreachable rows
    # get null stop_id below.
    addr_poi_idx = np.where(reachable, raw_poi_idx, 0).astype(int)

    # --- Snapping distance correction ---
    # pandana gives node-to-node distance only. Add the Euclidean distance
    # from each address to its nearest node, and from each stop to its
    # nearest node, so the total reflects the full door-to-stop walk.
    nodes = net.nodes_df
    node_x = nodes.loc[node_ids, "x"].to_numpy()
    node_y = nodes.loc[node_ids, "y"].to_numpy()

    cos_lat = np.cos(np.radians(addr_lats))
    addr_snap_m = np.sqrt(
        ((addr_lons - node_x) * 111_320.0 * cos_lat) ** 2
        + ((addr_lats - node_y) * 111_320.0) ** 2
    )

    # Stop snapping: distance from each stop to its nearest network node
    stop_lon_arr = np.array(stop_lons)
    stop_lat_arr = np.array(stop_lats)
    stop_node_ids = net.get_node_ids(stop_lon_arr, stop_lat_arr)
    stop_node_x = nodes.loc[stop_node_ids, "x"].to_numpy()
    stop_node_y = nodes.loc[stop_node_ids, "y"].to_numpy()
    cos_lat_s = np.cos(np.radians(stop_lat_arr))
    stop_snap_m = np.sqrt(
        ((stop_lon_arr - stop_node_x) * 111_320.0 * cos_lat_s) ** 2
        + ((stop_lat_arr - stop_node_y) * 111_320.0) ** 2
    )

    # Total = address snap + network path + stop snap
    addr_distances = network_distances + addr_snap_m + stop_snap_m[addr_poi_idx]

    # --- Warn if any addresses hit the maxdist clamp (T3h) ---
    clamped = int((addr_distances >= _MAX_DIST_METERS).sum())
    if clamped > 0:
        logger.warning(
            "%d address(es) hit the maxdist clamp (%gm) — possible data issue",
            clamped,
            _MAX_DIST_METERS,
        )

    mean_dist = float(np.nanmean(addr_distances))
    median_dist = float(np.nanmedian(addr_distances))
    max_dist = float(np.nanmax(addr_distances))
    logger.info(
        "Routing complete: mean=%.1fm median=%.1fm max=%.1fm",
        mean_dist,
        median_dist,
        max_dist,
    )

    # --- Map poi index → stop_id (T3e) ---
    # Unreachable addresses (NaN poi) get null stop_id instead of a wrong ID.
    nearest_stop_ids: list[str | None] = [
        stop_ids[i] if r else None for i, r in zip(addr_poi_idx, reachable)
    ]

    # --- Compute walk_minutes (T3f) ---
    pace = config.walking.pace_min_per_mile
    walk_minutes = (addr_distances / _METERS_PER_MILE) * pace

    # --- Join results back to addresses (T3g) ---
    result = addresses.with_columns(
        pl.Series("nearest_stop_distance_m", addr_distances, dtype=pl.Float64),
        pl.Series("walk_minutes", walk_minutes, dtype=pl.Float64),
        pl.Series("nearest_stop_id", nearest_stop_ids, dtype=pl.Utf8),
    )

    return result
