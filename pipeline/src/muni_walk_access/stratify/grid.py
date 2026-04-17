"""2D accessibility grid computation.

Computes per-neighbourhood and city-wide ``pct_within`` matrices indexed
by ``[freq_idx][walk_idx]``, where each cell is the fraction of residents
whose nearest stop meets both a frequency and walking-time threshold.
"""

from __future__ import annotations

import logging
from typing import Literal

import h3
import polars as pl

from muni_walk_access.config import Config
from muni_walk_access.emit.schemas import CityWide, HexCell, LensFlags, NeighborhoodGrid

# Scoring metric: "aggregate" = total trips/hr (higher is better),
# "headway" = best single-route headway (lower is better).
Metric = Literal["aggregate", "headway"]

logger = logging.getLogger(__name__)


def _build_meets_exprs(
    freq_thresholds: list[int],
    walk_thresholds: list[int],
    metric: Metric,
) -> tuple[list[str], list[pl.Expr]]:
    """Build the freq×walk boolean columns shared by neighborhood and hex paths.

    Returns ``(names, exprs)`` where each expression is True when a row meets
    the corresponding ``(freq_thresh, walk_thresh)`` pair. Null frequency values
    evaluate to False (``is_not_null() & …``).
    """
    names: list[str] = []
    exprs: list[pl.Expr] = []
    for fi, f_thresh in enumerate(freq_thresholds):
        if metric == "headway":
            freq_expr = pl.col("best_route_headway_min").is_not_null() & (
                pl.col("best_route_headway_min") <= f_thresh
            )
        else:
            trips_needed = 60.0 / f_thresh
            freq_expr = pl.col("trips_per_hour_peak") >= trips_needed
        for wi, w_thresh in enumerate(walk_thresholds):
            name = f"_m{fi}_{wi}"
            names.append(name)
            exprs.append((freq_expr & (pl.col("walk_minutes") <= w_thresh)).alias(name))
    return names, exprs


def compute_grid(
    stratified: pl.DataFrame,
    config: Config,
    metric: Metric = "aggregate",
) -> tuple[list[NeighborhoodGrid], CityWide]:
    """Compute 2D accessibility grids per neighbourhood and city-wide.

    Vectorized: pre-computes threshold boolean columns once, then uses a single
    ``group_by("neighborhood_id") + mean()`` pass instead of the old per-cell
    filter loop (Story 1.11 pattern, ~12,600 filter passes → 1 group_by per
    call).

    Args:
        stratified: DataFrame from ``aggregate_to_lenses`` with
            ``walk_minutes``, ``trips_per_hour_peak``,
            ``neighborhood_id``, ``neighborhood_name``, lens booleans.
        config: Pipeline configuration with grid axes.
        metric: Scoring metric — ``"aggregate"`` or ``"headway"``.

    Returns:
        ``(neighborhoods, city_wide)`` — neighbourhoods sorted ascending
        by ``id``. City-wide is computed directly from the global df (not by
        re-aggregating rounded per-neighborhood values).

    """
    freq_thresholds = config.grid.frequency_threshold_min
    walk_thresholds = config.grid.walking_minutes
    n_freq = len(freq_thresholds)
    n_walk = len(walk_thresholds)

    # Default indices
    freq_idx = freq_thresholds.index(config.grid.defaults.frequency_min)
    walk_idx = walk_thresholds.index(config.grid.defaults.walking_min)

    if len(stratified) == 0:
        empty: list[list[float]] = [[0.0] * n_walk for _ in range(n_freq)]
        return [], CityWide(pct_within=empty)

    names, exprs = _build_meets_exprs(freq_thresholds, walk_thresholds, metric)
    df = stratified.with_columns(exprs)

    # Aggregate per neighbourhood.
    # mean() on a boolean column treats nulls as nulls, so fill_null(False)
    # mirrors the old filter semantics where null freq never meets the threshold.
    mean_exprs: list[pl.Expr] = [
        pl.col(c).fill_null(value=False).cast(pl.Float64).mean().round(4).alias(c)
        for c in names
    ]
    grouped = (
        df.group_by("neighborhood_id")
        .agg(
            pl.first("neighborhood_name").alias("neighborhood_name"),
            pl.col("ej_community").any().alias("_ej"),
            pl.col("equity_strategy").any().alias("_eq"),
            pl.len().alias("_population"),
            *mean_exprs,
        )
        .sort("neighborhood_id")
    )

    neighborhoods: list[NeighborhoodGrid] = []
    for row in grouped.iter_rows(named=True):
        pct_within = [
            [row[f"_m{fi}_{wi}"] for wi in range(n_walk)] for fi in range(n_freq)
        ]
        neighborhoods.append(
            NeighborhoodGrid(
                id=row["neighborhood_id"],
                name=row["neighborhood_name"],
                population=row["_population"],
                lens_flags=LensFlags(
                    analysis_neighborhoods=True,
                    ej_communities=bool(row["_ej"]),
                    equity_strategy=bool(row["_eq"]),
                ),
                pct_within=pct_within,
            )
        )

    # City-wide: compute directly from the global df (no round-then-reweight).
    city_row = df.select(mean_exprs).row(0, named=True)
    city_pct = [
        [city_row[f"_m{fi}_{wi}"] for wi in range(n_walk)] for fi in range(n_freq)
    ]
    city_wide = CityWide(pct_within=city_pct)

    headline = city_pct[freq_idx][walk_idx]
    logger.info(
        "Grid: %d neighbourhoods, headline=%.4f (freq_idx=%d, walk_idx=%d)",
        len(neighborhoods),
        headline,
        freq_idx,
        walk_idx,
    )

    return neighborhoods, city_wide


def assign_hex_cells(
    stratified: pl.DataFrame,
    resolutions: list[int] | None = None,
) -> pl.DataFrame:
    """Assign H3 cell IDs to each row for multiple resolutions.

    This is the expensive step (Python loop over lat/lon). Call once per
    window and reuse the result for both aggregate and headway scoring.

    Args:
        stratified: DataFrame with ``latitude``, ``longitude`` columns.
        resolutions: H3 resolutions (default: 7-11 inclusive).

    Returns:
        DataFrame with ``h3_r{res}`` columns appended.

    """
    if resolutions is None:
        resolutions = list(range(7, 12))

    if len(stratified) == 0:
        return stratified

    lats = stratified["latitude"].to_list()
    lons = stratified["longitude"].to_list()
    return stratified.with_columns(
        [
            pl.Series(
                f"h3_r{res}",
                [h3.latlng_to_cell(lat, lon, res) for lat, lon in zip(lats, lons)],
            )
            for res in resolutions
        ]
    )


def compute_hex_grids(
    stratified: pl.DataFrame,
    config: Config,
    resolutions: list[int] | None = None,
    metric: Metric = "aggregate",
) -> dict[int, list[HexCell]]:
    """Compute 2D accessibility grids per H3 hex cell for multiple resolutions.

    Vectorized implementation: pre-computes threshold boolean columns once,
    then uses ``group_by``+``agg`` per resolution.

    If the DataFrame already contains ``h3_r{res}`` columns (from
    :func:`assign_hex_cells`), those are reused. Otherwise cell IDs are
    assigned on the fly (backward-compatible).

    Args:
        stratified: DataFrame with ``walk_minutes``,
            ``trips_per_hour_peak`` columns, and optionally pre-assigned
            ``h3_r{res}`` columns.
        config: Pipeline configuration with grid axes.
        resolutions: H3 resolutions to compute (default: 7-11 inclusive).
        metric: Scoring metric — ``"aggregate"`` or ``"headway"``.

    Returns:
        Mapping of resolution → sorted list of ``HexCell`` models.

    """
    if resolutions is None:
        resolutions = list(range(7, 12))

    freq_thresholds = config.grid.frequency_threshold_min
    walk_thresholds = config.grid.walking_minutes
    n_freq = len(freq_thresholds)
    n_walk = len(walk_thresholds)

    if len(stratified) == 0:
        return {res: [] for res in resolutions}

    # Assign H3 cells if not already present (backward compat)
    if f"h3_r{resolutions[0]}" not in stratified.columns:
        stratified = assign_hex_cells(stratified, resolutions)

    # Pre-compute the freq×walk boolean columns once (shared across resolutions).
    # Null freq → False (never meets threshold); see _build_meets_exprs.
    meets_names, meets_exprs = _build_meets_exprs(
        freq_thresholds, walk_thresholds, metric
    )
    df = stratified.with_columns(meets_exprs)

    # Aggregate expressions: mean of each boolean column (null → 0) per cell
    agg_exprs: list[pl.Expr] = [
        pl.col(c).fill_null(value=False).cast(pl.Float64).mean().round(4).alias(c)
        for c in meets_names
    ]
    agg_exprs.append(pl.len().alias("_population"))

    result: dict[int, list[HexCell]] = {}
    for res in resolutions:
        cell_col = f"h3_r{res}"

        grouped = df.group_by(cell_col).agg(agg_exprs).sort(cell_col)

        hex_cells: list[HexCell] = []
        for row in grouped.iter_rows(named=True):
            population: int = row["_population"]
            if population == 0:
                continue

            cell_id: str = row[cell_col]
            pct_within = [
                [row[f"_m{fi}_{wi}"] for wi in range(n_walk)] for fi in range(n_freq)
            ]
            center_lat, center_lon = h3.cell_to_latlng(cell_id)

            hex_cells.append(
                HexCell(
                    id=cell_id,
                    center_lat=round(center_lat, 6),
                    center_lon=round(center_lon, 6),
                    population=population,
                    pct_within=pct_within,
                )
            )

        logger.info("Hex grid r%d: %d cells", res, len(hex_cells))
        result[res] = hex_cells

    return result
