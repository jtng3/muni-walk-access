"""Frequency bin classification for GTFS stop-level trip counts."""

from __future__ import annotations

import polars as pl

from muni_walk_access.config import Config, FrequencyBin


def _validate_bins(bins: list[FrequencyBin]) -> None:
    """Validate bin list structure.

    Rules:
    - Exactly one catch-all bin (max_headway_min is None)
    - Catch-all must be last
    - Non-null bins must be ordered strictly ascending by max_headway_min
    """
    null_bins = [b for b in bins if b.max_headway_min is None]
    if len(null_bins) == 0:
        raise ValueError(
            "FrequencyConfig.bins must have exactly one catch-all bin"
            " (max_headway_min: null)"
        )
    if len(null_bins) > 1:
        raise ValueError(
            "FrequencyConfig.bins has multiple catch-all bins"
            " (max_headway_min: null); only one allowed"
        )
    if bins[-1].max_headway_min is not None:
        raise ValueError(
            "FrequencyConfig.bins catch-all bin (max_headway_min: null) must be last"
        )

    bounded = [b for b in bins if b.max_headway_min is not None]
    headways: list[int] = [
        b.max_headway_min for b in bounded if b.max_headway_min is not None
    ]
    if len(headways) != len(set(headways)) or headways != sorted(headways):
        raise ValueError(
            "FrequencyConfig.bins non-null bins must be strictly ascending"
            f" by max_headway_min (no duplicates); got {headways}"
        )


def classify_stops(df: pl.DataFrame, config: Config) -> pl.DataFrame:
    """Assign frequency_bin to each stop based on trips_per_hour_peak.

    Args:
        df: DataFrame with at minimum columns [stop_id, trips_per_hour_peak].
        config: Validated pipeline Config.

    Returns:
        Input DataFrame with additional column frequency_bin (str).

    Raises:
        ValueError: If bin configuration is invalid.

    """
    bins = config.frequency.bins
    _validate_bins(bins)

    # Build ordered list: bounded bins (ascending headway) + catch-all last
    bounded = [b for b in bins if b.max_headway_min is not None]
    catchall = next(b for b in bins if b.max_headway_min is None)

    # Compute headway: 60 / trips_per_hour_peak
    # Zero trips → infinite headway → catch-all bin
    result = df.with_columns(
        pl.when(pl.col("trips_per_hour_peak") <= 0)
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(60.0 / pl.col("trips_per_hour_peak"))
        .alias("_headway_min")
    )

    # Build the frequency_bin expression via nested when/then
    expr = pl.lit(catchall.id)
    # Apply in reverse order so first matching bin wins (like if/elif)
    for b in reversed(bounded):
        expr = (
            pl.when(
                pl.col("_headway_min").is_not_null()
                & (pl.col("_headway_min") <= b.max_headway_min)
            )
            .then(pl.lit(b.id))
            .otherwise(expr)
        )

    result = result.with_columns(expr.alias("frequency_bin")).drop("_headway_min")
    return result
