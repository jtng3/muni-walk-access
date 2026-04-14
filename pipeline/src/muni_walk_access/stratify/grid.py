"""2D accessibility grid computation.

Computes per-neighbourhood and city-wide ``pct_within`` matrices indexed
by ``[freq_idx][walk_idx]``, where each cell is the fraction of residents
whose nearest stop meets both a frequency and walking-time threshold.
"""

from __future__ import annotations

import logging

import polars as pl

from muni_walk_access.config import Config
from muni_walk_access.emit.schemas import CityWide, LensFlags, NeighborhoodGrid

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
