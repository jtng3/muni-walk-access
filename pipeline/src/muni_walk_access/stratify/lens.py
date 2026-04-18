"""Equity-lens spatial join and flag computation.

Assigns each routed address to its Analysis Neighborhood, Environmental
Justice Community membership, and SFMTA Equity Strategy membership via
spatial join against boundary datasets loaded through the generic
``BoundarySource`` dispatch (Story 5.3 T5).
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

import geopandas as gpd
import polars as pl

# Side-effect import: registers DataSFBoundarySource in BOUNDARY_SOURCES.
# Required even though this module doesn't use the class by name.
import muni_walk_access.ingest.sources.datasf  # noqa: F401
from muni_walk_access.config import Config
from muni_walk_access.ingest.boundaries import (
    _apply_lens_filter,
    get_boundary_source,
)

if TYPE_CHECKING:
    from muni_walk_access.run_context import RunContext

logger = logging.getLogger(__name__)

# Column in the Analysis Neighborhoods GeoJSON that holds the name.
# TODO(Story 5.3 T8): lift into LensConfig.name_field (already exists; this
# constant just keeps the byte-identical gate steady until the T8 sweep).
_ANA_NAME_COL = "nhood"


def slugify_neighborhood(name: str) -> str:
    """Convert a neighbourhood name to a kebab-case slug.

    **CONTRACT — DO NOT "IMPROVE" THIS FUNCTION.**

    This is the single source of truth for neighbourhood ID generation. The
    ``id`` fields in both ``grid.json`` and ``neighborhoods.geojson`` originate
    here: ``aggregate_to_lenses`` (below) stamps the slug onto every row as
    ``neighborhood_id`` — which ``compute_grid`` then forwards into
    ``NeighborhoodGrid.id`` — and ``write_neighborhoods_geojson`` calls this
    function directly when writing feature IDs. The frontend joins those two
    files on that ``id`` — any slug-rule change here silently breaks the join
    and leaves the map rendering with missing or mis-coloured polygons.

    If you need different slug rules for a future city adapter (Story 5.3+),
    do NOT edit this function. Pass a per-city slugifier through the adapter
    config so SF's rules stay pinned.

    Rules (current):
    - lowercase
    - strip apostrophes
    - collapse any run of non-[a-z0-9] characters to a single ``-``
    - strip leading/trailing ``-``
    """
    s = name.lower()
    s = s.replace("'", "")  # strip apostrophes before slugifying
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s


def _fetch_boundaries(
    config: Config,
    ctx: RunContext | None = None,
) -> dict[str, gpd.GeoDataFrame]:
    """Fetch + filter all lens boundary GeoDataFrames via BoundarySource dispatch.

    For each lens in ``config.lenses``, resolves the source adapter via
    ``lens.source_kind`` (e.g. "datasf" → :class:`DataSFBoundarySource`),
    then applies the generic attribute-filter engine
    (:func:`_apply_lens_filter`) using the config's filter/score rules.

    The EJ Communities SF-specific ``score >= 21`` filter that used to live
    here is now config-driven (``score_field: score`` + ``score_threshold:
    21`` on the EJ lens in ``config.yaml``) and applied generically.
    """
    boundaries: dict[str, gpd.GeoDataFrame] = {}
    for lens in config.lenses:
        source = get_boundary_source(lens.source_kind)()
        gdf = source.fetch(lens, ctx)
        gdf = _apply_lens_filter(gdf, lens)
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
    ctx: RunContext | None = None,
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
        ctx: Optional run context threaded to :class:`BoundarySource`
            adapters so they can record lens-boundary dataset timestamps
            into ``ctx.datasf_timestamps`` (Story 5.3 T5 / AC-12).

    Returns a Polars DataFrame with the original routing columns plus:
    ``neighborhood_id``, ``neighborhood_name``, one boolean per
    boundary-membership lens (column name = ``lens.source_column``),
    ``trips_per_hour_peak``.

    Addresses that fall outside the name lens are excluded with a log
    INFO message.

    """
    # The "name lens" is the single lens with ``source_column is None`` —
    # it contributes ``neighborhood_name`` (a polygon attribute) rather
    # than a per-address boolean. Boundary-membership lenses all declare
    # ``source_column`` and produce a boolean column of that name.
    name_lens = next(
        (lens_cfg for lens_cfg in config.lenses if lens_cfg.source_column is None),
        None,
    )
    if name_lens is None:
        raise ValueError(
            "config.lenses must contain exactly one name lens "
            "(source_column is None); none found"
        )
    boolean_lenses = [
        lens_cfg for lens_cfg in config.lenses if lens_cfg.source_column is not None
    ]

    if len(routing_result) == 0:
        empty_cols: list[pl.Expr] = [
            pl.lit(None).cast(pl.Utf8).alias("neighborhood_id"),
            pl.lit(None).cast(pl.Utf8).alias("neighborhood_name"),
        ]
        for lens in boolean_lenses:
            empty_cols.append(pl.lit(None).cast(pl.Boolean).alias(lens.source_column))
        empty_cols.append(pl.lit(None).cast(pl.Float64).alias("trips_per_hour_peak"))
        return routing_result.with_columns(*empty_cols)

    boundaries = _fetch_boundaries(config, ctx)

    # Convert to GeoDataFrame
    addr_pd = routing_result.to_pandas()
    addr_gdf = gpd.GeoDataFrame(
        addr_pd,
        geometry=gpd.points_from_xy(addr_pd["longitude"], addr_pd["latitude"]),
        crs="EPSG:4326",
    )

    # --- Name lens: get polygon name per address ---
    ana_bnd = boundaries[name_lens.id]
    # Rename to avoid collision with source 'nhood' column in the routing result
    result_col = "neighborhood_name"
    ana_slim = ana_bnd[[_ANA_NAME_COL, "geometry"]].rename(
        columns={_ANA_NAME_COL: result_col}
    )
    ana_joined = gpd.sjoin(addr_gdf, ana_slim, how="left", predicate="within")
    ana_joined = ana_joined[~ana_joined.index.duplicated(keep="first")]
    neighbourhood_names = ana_joined[result_col]

    # Assemble result on the original pandas frame
    result_pd = addr_pd.copy()
    result_pd["neighborhood_name"] = neighbourhood_names.values

    # --- Boundary-membership lenses: one boolean column per lens ---
    for lens in boolean_lenses:
        result_pd[lens.source_column] = _sjoin_boolean(addr_gdf, boundaries[lens.id])

    # Exclude addresses outside the name lens (no neighbourhood match)
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
        select_cols = [
            "stop_id",
            pl.col("total_trips_per_hour").alias("trips_per_hour_peak"),
        ]
        if "best_route_headway_min" in stops_df.columns:
            select_cols.append("best_route_headway_min")
        stops_pd = window_stops.select(select_cols).to_pandas()
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

    # Build final column list. Order: original routing columns, then
    # neighborhood_id + neighborhood_name, then one column per
    # boundary-membership lens (in config order, matching how downstream
    # readers iterate), then the stop-frequency fields.
    orig_cols = list(routing_result.columns)
    new_cols: list[str] = ["neighborhood_id", "neighborhood_name"]
    new_cols.extend(
        lens.source_column for lens in boolean_lenses if lens.source_column is not None
    )
    new_cols.extend(["trips_per_hour_peak", "best_route_headway_min"])
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
    select_cols = [
        "stop_id",
        pl.col("total_trips_per_hour").alias("_new_tph"),
    ]
    has_headway = "best_route_headway_min" in summary_df.columns
    if has_headway:
        select_cols.append(pl.col("best_route_headway_min").alias("_new_headway"))
    freq_lookup = window_stops.select(select_cols)

    drop_cols = ["trips_per_hour_peak"]
    rename_map: dict[str, str] = {"_new_tph": "trips_per_hour_peak"}
    if has_headway:
        if "best_route_headway_min" in stratified.columns:
            drop_cols.append("best_route_headway_min")
        rename_map["_new_headway"] = "best_route_headway_min"

    result = (
        stratified.drop(drop_cols)
        .join(
            freq_lookup,
            left_on="nearest_stop_id",
            right_on="stop_id",
            how="left",
        )
        .rename(rename_map)
    )

    return result


def compute_lens_flags(
    stratified: pl.DataFrame,
    config: Config,
) -> list[dict[str, Any]]:
    """Compute per-neighbourhood equity-lens flags.

    Returns a list of dicts sorted by ``neighborhood_id`` with keys:
    ``neighborhood_id``, ``neighborhood_name``, ``lens_flags`` (dict),
    ``lens_flag_count`` (int 0 to len(config.lenses)).

    Story 5.3 T6: iterates ``config.lenses`` generically. Each lens with
    a ``source_column`` contributes an aggregated ``.any()`` over that
    column; lenses without ``source_column`` (e.g. the name-providing
    ``analysis_neighborhoods`` lens) get a constant ``True`` flag —
    addresses outside all boundaries are filtered out upstream so every
    remaining row is inside the name lens.
    """
    if len(stratified) == 0:
        return []

    # Internal alias pattern matches compute_grid: `_lens_<lens.id>`.
    # Missing-column diagnostic mirrors compute_grid: warn-and-skip rather
    # than raising an opaque polars ColumnNotFoundError.
    lens_agg_exprs: list[pl.Expr] = []
    for lens in config.lenses:
        if lens.source_column is None:
            continue
        if lens.source_column not in stratified.columns:
            logger.warning(
                "Lens %s: source_column %r missing from stratified columns "
                "(%s); skipping lens aggregation — flag will be False",
                lens.id,
                lens.source_column,
                stratified.columns,
            )
            continue
        lens_agg_exprs.append(
            pl.col(lens.source_column).any().alias(f"_lens_{lens.id}")
        )

    grouped = stratified.group_by("neighborhood_id").agg(
        pl.first("neighborhood_name"),
        *lens_agg_exprs,
    )

    results: list[dict[str, Any]] = []
    for row in grouped.iter_rows(named=True):
        # Dict keys are lens.id in config-declared order (byte-identical gate).
        flags: dict[str, bool] = {}
        for lens in config.lenses:
            alias = f"_lens_{lens.id}"
            if lens.source_column is None:
                flags[lens.id] = True
            elif alias not in row:
                flags[lens.id] = False
            else:
                flags[lens.id] = bool(row[alias])
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
