"""DataSF SODA API fetcher + `DataSFAddressSource` adapter (Story 5.3).

Story 5.3 T3 moves the DataSF helpers from `ingest/datasf.py` to this
module-level location so the generic `AddressSource` Protocol can be
implemented at the bottom. The legacy `ingest/datasf.py` now aliases
this module via `sys.modules` — see its docstring. T7 deletes the alias
file entirely.

Module globals (`_upstream_fallback`, `_datasf_timestamps`) live here
rather than on the adapter class because the GTFS and OSM fetchers also
write to them; keeping one global source of truth during the transition
period avoids split-brain state.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import geopandas as gpd
import httpx
import polars as pl

from muni_walk_access.config import Config, IngestConfig
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.boundaries import BOUNDARY_SOURCES
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.contracts import validate_wgs84
from muni_walk_access.ingest.sources import ADDRESS_SOURCES

if TYPE_CHECKING:
    from muni_walk_access.config import LensConfig
    from muni_walk_access.run_context import RunContext

logger = logging.getLogger(__name__)

SODA_BASE = "https://data.sfgov.org/resource"
SODA_PAGE_SIZE = 50_000
CACHE_SUBDIR_TABULAR = "datasf"
CACHE_SUBDIR_GEO = "datasf"

# Residential filter constants (Story 1.6)
_EAS_DATASET_ID = "ramy-di5m"  # EAS with Units (per Luke's data sources)
# Assessor Historical Secured Property Tax Rolls
_INTERIM_PARCEL_DATASET_ID = "wv5m-vpq2"
_TBD_SENTINEL = "TBD_FROM_LUKE"
_SPIKE_MEMO_PATH = "pipeline/docs/residential-filter-spike.md"

_upstream_fallback: bool = False
_datasf_timestamps: dict[str, str] = {}


# Public surface — these are the names other modules and tests import via
# the legacy `ingest.datasf` path (sys.modules aliased to this module). T7
# trims this list as call sites migrate to ctx.* and the new factory.
__all__ = [
    "DataSFAddressSource",
    "SODA_BASE",
    "_EAS_DATASET_ID",
    "_INTERIM_PARCEL_DATASET_ID",
    "_TBD_SENTINEL",
    "_datasf_timestamps",
    "_record_timestamp",
    "_upstream_fallback",
    "fetch_datasf_metadata",
    "fetch_geospatial",
    "fetch_residential_addresses",
    "fetch_tabular",
    "get_datasf_timestamps",
    "set_upstream_fallback",
    "was_fallback_used",
]


def was_fallback_used() -> bool:
    """Return True if any dataset used cached fallback due to upstream failure."""
    return _upstream_fallback


def set_upstream_fallback(ctx: RunContext | None = None) -> None:
    """Mark that an upstream fallback was used (callable from other ingest modules).

    Story 5.3 dual-write: when ``ctx`` is supplied, mirrors the flag onto
    ``ctx.upstream_fallback``. The legacy module global remains the source
    of truth until T7 deletes it; until then both must stay in sync.
    """
    global _upstream_fallback
    _upstream_fallback = True
    if ctx is not None:
        ctx.upstream_fallback = True


def get_datasf_timestamps() -> dict[str, str]:
    """Return mapping of dataset_id -> yyyymmdd for provenance."""
    return dict(_datasf_timestamps)


def fetch_datasf_metadata(
    dataset_ids: list[str],
    client: httpx.Client | None = None,
) -> dict[str, str]:
    """Fetch rowsUpdatedAt from Socrata metadata API for each dataset.

    Returns mapping of dataset_id -> ISO datetime string.
    """
    own_client = client is None
    _client = client if client is not None else httpx.Client(timeout=30.0)
    result: dict[str, str] = {}
    try:
        for did in dataset_ids:
            try:
                resp = _client.get(f"https://data.sfgov.org/api/views/{did}.json")
                resp.raise_for_status()
                meta = resp.json()
                updated = meta.get("rowsUpdatedAt")
                if updated:
                    from datetime import datetime, timezone

                    dt = datetime.fromtimestamp(int(updated), tz=timezone.utc)
                    result[did] = dt.isoformat()
            except Exception as exc:
                logger.warning("Failed to fetch metadata for %s: %s", did, exc)
    finally:
        if own_client:
            _client.close()
    return result


def _record_timestamp(
    dataset_id: str, path: Path, ctx: RunContext | None = None
) -> None:
    """Extract and store the date suffix from a cache file path.

    Story 5.3 dual-write: when ``ctx`` is supplied, mirrors the timestamp
    onto ``ctx.datasf_timestamps``. T7 collapses to ctx-only.
    """
    stem = path.stem  # e.g. "i28k-bkz6-20260412"
    parts = stem.rsplit("-", 1)
    if len(parts) == 2:
        _datasf_timestamps[dataset_id] = parts[1]
        if ctx is not None:
            ctx.datasf_timestamps[dataset_id] = parts[1]


def fetch_tabular(
    dataset_id: str,
    config: IngestConfig,
    client: httpx.Client | None = None,
    ctx: RunContext | None = None,
) -> pl.DataFrame:
    """Fetch a tabular SODA dataset as a Polars DataFrame.

    Uses pagination ($limit/$offset) to bypass SODA's 1 000-row default cap.
    Caches result as Parquet. On HTTP failure, returns cached data if available
    and sets the upstream-fallback flag (dual-written to ``ctx`` when supplied).
    Raises IngestError if no cache exists and the upstream is unreachable.
    """
    cache = CacheManager(root=config.cache_dir, ttl_days=config.cache_ttl_days)
    fresh = cache.get(CACHE_SUBDIR_TABULAR, dataset_id)
    if fresh is not None:
        _record_timestamp(dataset_id, fresh, ctx)
        return pl.read_parquet(fresh)

    # Need to fetch
    own_client = client is None
    _client: httpx.Client = client if client is not None else httpx.Client(timeout=60.0)

    try:
        pages: list[pl.DataFrame] = []
        offset = 0
        while True:
            url = f"{SODA_BASE}/{dataset_id}.csv"
            params: dict[str, Any] = {"$limit": SODA_PAGE_SIZE, "$offset": offset}
            resp = _client.get(url, params=params)
            resp.raise_for_status()
            text = resp.text
            if not text.strip():
                break
            # Read all columns as String to avoid cross-page schema conflicts
            # (SODA pages independently contain mixed types like block "0026T").
            page = pl.read_csv(
                io.StringIO(text),
                infer_schema_length=0,
                null_values=["NA"],
            )
            if page.is_empty():
                break
            pages.append(page)
            if len(page) < SODA_PAGE_SIZE:
                break
            offset += SODA_PAGE_SIZE

        if not pages:
            logger.warning("SODA returned no data for dataset %s", dataset_id)
            return pl.DataFrame()
        df = pl.concat(pages)
        buf = io.BytesIO()
        df.write_parquet(buf)
        path = cache.put(CACHE_SUBDIR_TABULAR, dataset_id, buf.getvalue(), "parquet")
        _record_timestamp(dataset_id, path, ctx)
        return df

    except (httpx.HTTPError, httpx.TransportError) as exc:
        stale = cache.get_any(CACHE_SUBDIR_TABULAR, dataset_id)
        if stale is not None:
            logger.warning(
                "DataSF fetch failed for %s (%s); returning stale cache %s",
                dataset_id,
                exc,
                stale,
            )
            set_upstream_fallback(ctx)
            _record_timestamp(dataset_id, stale, ctx)
            return pl.read_parquet(stale)
        raise IngestError(
            dataset_id,
            f"HTTP error and no local cache: {exc}. "
            "Warm the cache with network access first.",
        ) from exc
    finally:
        if own_client:
            _client.close()


def fetch_geospatial(
    dataset_id: str,
    config: IngestConfig,
    client: httpx.Client | None = None,
    ctx: RunContext | None = None,
    *,
    limit: int | None = None,
) -> Path:
    """Fetch a geospatial SODA dataset as a cached GeoJSON file.

    Returns the Path to the cached .geojson file. On failure uses stale cache
    (dual-writing the upstream-fallback flag to ``ctx`` when supplied) or
    raises IngestError.

    ``limit`` overrides SODA's default 1 000-row cap. Lens boundary
    datasets (e.g. EJ Communities at 2 700+ tracts) pass ``limit=50_000``
    so the full dataset is cached; other callers leave it ``None`` to
    accept SODA defaults.
    """
    cache = CacheManager(root=config.cache_dir, ttl_days=config.cache_ttl_days)
    fresh = cache.get(CACHE_SUBDIR_GEO, dataset_id)
    if fresh is not None:
        _record_timestamp(dataset_id, fresh, ctx)
        return fresh

    own_client = client is None
    _client: httpx.Client = client if client is not None else httpx.Client(timeout=60.0)

    try:
        url = f"{SODA_BASE}/{dataset_id}.geojson"
        params: dict[str, Any] = {"$limit": limit} if limit is not None else {}
        resp = _client.get(url, params=params)
        resp.raise_for_status()
        path = cache.put(CACHE_SUBDIR_GEO, dataset_id, resp.content, "geojson")
        _record_timestamp(dataset_id, path, ctx)
        return path

    except (httpx.HTTPError, httpx.TransportError) as exc:
        stale = cache.get_any(CACHE_SUBDIR_GEO, dataset_id)
        if stale is not None:
            logger.warning(
                "DataSF geo fetch failed for %s (%s); returning stale cache %s",
                dataset_id,
                exc,
                stale,
            )
            set_upstream_fallback(ctx)
            _record_timestamp(dataset_id, stale, ctx)
            return stale
        raise IngestError(
            dataset_id,
            f"HTTP error and no local cache: {exc}. "
            "Warm the cache with network access first.",
        ) from exc
    finally:
        if own_client:
            _client.close()


def fetch_residential_addresses(
    config: Config,
    client: httpx.Client | None = None,
    ctx: RunContext | None = None,
) -> pl.DataFrame:
    """Fetch EAS base addresses filtered to residential parcels only.

    Joins EAS addresses (``ramy-di5m``) against a parcel dataset on
    ``parcel_number`` and filters to rows whose ``use_code`` matches
    ``config.residential_filter.use_codes_residential``.

    When ``parcel_dataset_id`` is the ``TBD_FROM_LUKE`` placeholder, falls back to
    the interim Assessor Historical Tax Rolls dataset (wv5m-vpq2) and emits a
    WARNING. Update ``config.yaml`` once Luke Armbruster confirms the real dataset.

    Args:
        config: Root pipeline configuration; needs ``.ingest`` and
            ``.residential_filter``.
        client: Optional httpx.Client injected for testability; created if None.
        ctx: Optional RunContext for dual-writing fallback flag and dataset
            timestamps onto the run-scoped state. Story 5.3 transition aid.

    Returns:
        DataFrame of EAS addresses annotated with ``use_code``, filtered to
        residential rows. May be empty if the parcel join finds no overlap.

    Raises:
        IngestError: If either the EAS or parcel dataset cannot be fetched and
            no local cache is available.

    """
    parcel_id = config.residential_filter.parcel_dataset_id
    if parcel_id == _TBD_SENTINEL:
        parcel_id = _INTERIM_PARCEL_DATASET_ID
        logger.warning(
            "residential_filter.parcel_dataset_id is placeholder '%s'; "
            "using interim dataset %s. "
            "Update config.yaml once Luke confirms the dataset. "
            "See spike memo: %s",
            _TBD_SENTINEL,
            _INTERIM_PARCEL_DATASET_ID,
            _SPIKE_MEMO_PATH,
        )
    else:
        logger.info("Using configured parcel dataset: %s", parcel_id)

    eas = fetch_tabular(_EAS_DATASET_ID, config.ingest, client=client, ctx=ctx)
    parcels = fetch_tabular(parcel_id, config.ingest, client=client, ctx=ctx)

    # Assessor historical data spans multiple years — keep only the most recent roll.
    # String max works correctly for 4-digit year values ("2024" > "2023").
    if "closed_roll_year" in parcels.columns:
        max_year = parcels["closed_roll_year"].max()
        parcels = parcels.filter(pl.col("closed_roll_year") == max_year)

    joined = eas.join(
        parcels.select(["parcel_number", "use_code"]),
        on="parcel_number",
        how="inner",
    )
    residential = joined.filter(
        pl.col("use_code").is_in(config.residential_filter.use_codes_residential)
    )
    logger.info(
        "Residential filter: %d/%d EAS addresses kept (%d had parcel match)",
        len(residential),
        len(eas),
        len(joined),
    )
    return residential


class DataSFAddressSource:
    """AddressSource implementation for SF (DataSF EAS + Assessor join).

    Owns no state: every value it needs comes from ``ctx`` (which carries
    the ``Config``). Constructed once at pipeline startup via the factory,
    then `.fetch(ctx)` runs the SF-specific EAS × parcel join.
    """

    def fetch(self, ctx: RunContext) -> pl.DataFrame:
        """Fetch + validate SF residential addresses.

        Returns the same DataFrame shape that
        :func:`fetch_residential_addresses` has always returned (EAS columns
        + ``use_code``). The canonical :class:`ResidentialAddress`-shaped
        rename is deferred to Story 5-4 when Philly's OPA adapter forces
        a consistent naming contract — renaming now would break the
        byte-identical gate on downstream routing/stratify.

        The boundary check that IS applied here (cheap + aligned with 5-1
        contracts): :func:`validate_wgs84` on the coordinate columns after
        a non-strict cast to Float64 (EAS rows arrive as strings because
        SODA responses are loaded with ``infer_schema_length=0``; the
        downstream Float64 cast proper happens in ``route.nearest_stop``).

        Nulls are intentionally NOT dropped before validation: validate_wgs84
        already handles a Float64 series with nulls by computing min/max
        across the non-null subset. A fully-corrupt payload (every value
        casts to null) raises "no non-null values" — which is what we want
        instead of silently shortcircuiting on an empty series.
        """
        df = fetch_residential_addresses(ctx.config, ctx=ctx)
        if not df.is_empty():
            lats = df["latitude"].cast(pl.Float64, strict=False)
            lons = df["longitude"].cast(pl.Float64, strict=False)
            validate_wgs84(lats, lons)
        return df


ADDRESS_SOURCES["datasf"] = DataSFAddressSource


# ---------------------------------------------------------------------------
# DataSFBoundarySource — Story 5.3 T5a
# ---------------------------------------------------------------------------

# Row limit for SODA lens-boundary fetches. SF's EJ Communities dataset has
# 2 700+ tracts; SODA's default 1 000-row cap would silently truncate it.
_LENS_BOUNDARY_ROW_LIMIT = 50_000


class DataSFBoundarySource:
    """BoundarySource implementation for SF lens boundaries on DataSF.

    Wraps :func:`fetch_geospatial` with a 50 000-row ``$limit`` override
    (SODA defaults to 1 000, which truncates the EJ Communities dataset).
    Records each lens dataset's Last-Modified date into
    ``ctx.datasf_timestamps`` via :func:`_record_timestamp` — closing the
    AC-12 provenance gap where lens boundary timestamps were previously
    lost (only EAS + parcel timestamps were captured).

    The returned GeoDataFrame is CRS-normalized to EPSG:4326 but NOT
    attribute-filtered — attribute filtering (e.g. SF's EJ score ≥ 21) is
    a generic concern handled by ``_apply_lens_filter`` in
    ``muni_walk_access.ingest.boundaries``, so every BoundarySource impl
    shares the same filter semantics.
    """

    def fetch(
        self, lens: LensConfig, ctx: RunContext | None = None
    ) -> gpd.GeoDataFrame:
        """Fetch + CRS-normalize a DataSF lens boundary.

        Requires ``ctx`` (despite the Protocol's ``ctx: RunContext | None``
        optional signature, kept for the Story 5.3 transition-state
        pattern). DataSF adapters need ``ctx.config.ingest`` for cache
        configuration and ``ctx`` for provenance timestamp recording via
        :func:`_record_timestamp`.
        """
        if ctx is None or ctx.config is None:
            raise ValueError(
                "DataSFBoundarySource.fetch requires ctx with a bound Config — "
                "construct via `RunContext.from_config(...)` in __main__.py and "
                "thread through `aggregate_to_lenses(ctx=ctx)`."
            )
        path = fetch_geospatial(
            lens.datasf_id,
            ctx.config.ingest,
            ctx=ctx,
            limit=_LENS_BOUNDARY_ROW_LIMIT,
        )
        gdf: gpd.GeoDataFrame = gpd.read_file(path)
        # CRS-less sources (e.g. Philly OPA exports via GenericURLBoundarySource
        # in Story 5-4) require a declarative `set_crs` — feeding them into
        # `to_crs` directly raises inside pyproj. DataSF's SODA GeoJSON endpoint
        # always ships WGS84 metadata, so SF always hits the elif path.
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        return gdf


BOUNDARY_SOURCES["datasf"] = DataSFBoundarySource
