"""2D accessibility grid computation.

Computes per-neighbourhood and city-wide ``pct_within`` matrices indexed
by ``[freq_idx][walk_idx]``, where each cell is the fraction of residents
whose nearest stop meets both a frequency and walking-time threshold.
"""

from __future__ import annotations

import logging

import h3
import polars as pl

from muni_walk_access.config import Config
from muni_walk_access.emit.schemas import CityWide, HexCell, LensFlags, NeighborhoodGrid

logger = logging.getLogger(__name__)


def _compute_neighbourhood_grid(
    nbhd_df: pl.DataFrame,
    freq_thresholds: list[int],
    walk_thresholds: list[int],
) -> list[list[float]]:
    """Compute the pct_within matrix for one neighbourhood."""
    population = len(nbhd_df)
    if population == 0:
        return [[0.0] * len(walk_thresholds) for _ in freq_thresholds]

    grid: list[list[float]] = []
    for f_thresh in freq_thresholds:
        trips_needed = 60.0 / f_thresh
        row: list[float] = []
        for w_thresh in walk_thresholds:
            count = nbhd_df.filter(
                (pl.col("trips_per_hour_peak") >= trips_needed)
                & (pl.col("walk_minutes") <= w_thresh)
            ).height
            row.append(round(count / population, 4))
        grid.append(row)
    return grid


def compute_grid(
    stratified: pl.DataFrame,
    config: Config,
) -> tuple[list[NeighborhoodGrid], CityWide]:
    """Compute 2D accessibility grids per neighbourhood and city-wide.

    Args:
        stratified: DataFrame from ``aggregate_to_lenses`` with
            ``walk_minutes``, ``trips_per_hour_peak``,
            ``neighborhood_id``, ``neighborhood_name``, lens booleans.
        config: Pipeline configuration with grid axes.

    Returns:
        ``(neighborhoods, city_wide)`` — neighbourhoods sorted ascending
        by ``id``.

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

    # Group by neighbourhood, sorted by id
    nbhd_ids = sorted(stratified["neighborhood_id"].unique().to_list())

    neighborhoods: list[NeighborhoodGrid] = []
    total_pop = 0
    city_sum: list[list[float]] = [[0.0] * n_walk for _ in range(n_freq)]

    for nbhd_id in nbhd_ids:
        nbhd_df = stratified.filter(pl.col("neighborhood_id") == nbhd_id)
        population = nbhd_df.height
        total_pop += population
        nbhd_name: str = nbhd_df["neighborhood_name"][0]

        # Lens flags
        ej_flag = bool(nbhd_df["ej_community"].any())
        eq_flag = bool(nbhd_df["equity_strategy"].any())
        lens_flags = LensFlags(
            analysis_neighborhoods=True,
            ej_communities=ej_flag,
            equity_strategy=eq_flag,
        )

        pct_within = _compute_neighbourhood_grid(
            nbhd_df, freq_thresholds, walk_thresholds
        )

        # Accumulate weighted sums for city-wide
        for fi in range(n_freq):
            for wi in range(n_walk):
                city_sum[fi][wi] += pct_within[fi][wi] * population

        neighborhoods.append(
            NeighborhoodGrid(
                id=nbhd_id,
                name=nbhd_name,
                population=population,
                lens_flags=lens_flags,
                pct_within=pct_within,
            )
        )

    # City-wide population-weighted average
    if total_pop == 0:
        city_pct: list[list[float]] = [[0.0] * n_walk for _ in range(n_freq)]
    else:
        city_pct = [
            [round(city_sum[fi][wi] / total_pop, 4) for wi in range(n_walk)]
            for fi in range(n_freq)
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


def compute_hex_grids(
    stratified: pl.DataFrame,
    config: Config,
    resolutions: list[int] | None = None,
) -> dict[int, list[HexCell]]:
    """Compute 2D accessibility grids per H3 hex cell for multiple resolutions.

    Vectorized implementation: assigns all resolution cell IDs in one pass,
    pre-computes threshold boolean columns once, then uses ``group_by``+``agg``
    per resolution — avoiding per-cell Python filter loops.

    Args:
        stratified: DataFrame from ``aggregate_to_lenses`` with
            ``latitude``, ``longitude``, ``walk_minutes``,
            ``trips_per_hour_peak`` columns.
        config: Pipeline configuration with grid axes.
        resolutions: H3 resolutions to compute (default: 7-11 inclusive).

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

    # Assign all resolution cell ID columns in a single Python pass
    lats = stratified["latitude"].to_list()
    lons = stratified["longitude"].to_list()
    df = stratified.with_columns(
        [
            pl.Series(
                f"h3_r{res}",
                [h3.latlng_to_cell(lat, lon, res) for lat, lon in zip(lats, lons)],
            )
            for res in resolutions
        ]
    )

    # Pre-compute the freq×walk boolean columns once (shared across all resolutions).
    # Null trips_per_hour_peak → null AND anything → null → fill_null(False) = False,
    # consistent with the existing filter-based approach in _compute_neighbourhood_grid.
    meets_names: list[str] = []
    meets_exprs: list[pl.Expr] = []
    for fi, f_thresh in enumerate(freq_thresholds):
        trips_needed = 60.0 / f_thresh
        freq_expr = pl.col("trips_per_hour_peak") >= trips_needed
        for wi, w_thresh in enumerate(walk_thresholds):
            name = f"_m{fi}_{wi}"
            meets_names.append(name)
            meets_exprs.append(
                (freq_expr & (pl.col("walk_minutes") <= w_thresh)).alias(name)
            )

    df = df.with_columns(meets_exprs)

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
