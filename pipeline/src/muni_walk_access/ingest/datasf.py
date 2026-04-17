"""DataSF SODA API fetcher with local filesystem cache."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl

from muni_walk_access.config import Config, IngestConfig
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.cache import CacheManager

if TYPE_CHECKING:
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
) -> Path:
    """Fetch a geospatial SODA dataset as a cached GeoJSON file.

    Returns the Path to the cached .geojson file. On failure uses stale cache
    (dual-writing the upstream-fallback flag to ``ctx`` when supplied) or
    raises IngestError.
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
        resp = _client.get(url)
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

    Joins EAS addresses (3mea-di5p) against a parcel dataset on ``parcel_number``
    and filters to rows whose ``use_code`` matches
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
