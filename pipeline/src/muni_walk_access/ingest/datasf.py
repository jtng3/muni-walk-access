"""DataSF SODA API fetcher with local filesystem cache."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import httpx
import polars as pl

from muni_walk_access.config import IngestConfig
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.cache import CacheManager

logger = logging.getLogger(__name__)

SODA_BASE = "https://data.sfgov.org/resource"
SODA_PAGE_SIZE = 50_000
CACHE_SUBDIR_TABULAR = "datasf"
CACHE_SUBDIR_GEO = "datasf"

_upstream_fallback: bool = False
_datasf_timestamps: dict[str, str] = {}


def was_fallback_used() -> bool:
    """Return True if any dataset used cached fallback due to upstream failure."""
    return _upstream_fallback


def set_upstream_fallback() -> None:
    """Mark that an upstream fallback was used (callable from other ingest modules)."""
    global _upstream_fallback
    _upstream_fallback = True


def get_datasf_timestamps() -> dict[str, str]:
    """Return mapping of dataset_id -> yyyymmdd for provenance."""
    return dict(_datasf_timestamps)


def _record_timestamp(dataset_id: str, path: Path) -> None:
    """Extract and store the date suffix from a cache file path."""
    stem = path.stem  # e.g. "i28k-bkz6-20260412"
    parts = stem.rsplit("-", 1)
    if len(parts) == 2:
        _datasf_timestamps[dataset_id] = parts[1]


def fetch_tabular(
    dataset_id: str,
    config: IngestConfig,
    client: httpx.Client | None = None,
) -> pl.DataFrame:
    """Fetch a tabular SODA dataset as a Polars DataFrame.

    Uses pagination ($limit/$offset) to bypass SODA's 1 000-row default cap.
    Caches result as Parquet. On HTTP failure, returns cached data if available
    and sets the module-level upstream_fallback flag. Raises IngestError if
    no cache exists and the upstream is unreachable.
    """
    global _upstream_fallback

    cache = CacheManager(root=config.cache_dir, ttl_days=config.cache_ttl_days)
    fresh = cache.get(CACHE_SUBDIR_TABULAR, dataset_id)
    if fresh is not None:
        _record_timestamp(dataset_id, fresh)
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
            page = pl.read_csv(io.StringIO(text))
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
        _record_timestamp(dataset_id, path)
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
            _upstream_fallback = True
            _record_timestamp(dataset_id, stale)
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
) -> Path:
    """Fetch a geospatial SODA dataset as a cached GeoJSON file.

    Returns the Path to the cached .geojson file. On failure uses stale cache
    or raises IngestError.
    """
    global _upstream_fallback

    cache = CacheManager(root=config.cache_dir, ttl_days=config.cache_ttl_days)
    fresh = cache.get(CACHE_SUBDIR_GEO, dataset_id)
    if fresh is not None:
        _record_timestamp(dataset_id, fresh)
        return fresh

    own_client = client is None
    _client: httpx.Client = client if client is not None else httpx.Client(timeout=60.0)

    try:
        url = f"{SODA_BASE}/{dataset_id}.geojson"
        resp = _client.get(url)
        resp.raise_for_status()
        path = cache.put(CACHE_SUBDIR_GEO, dataset_id, resp.content, "geojson")
        _record_timestamp(dataset_id, path)
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
            _upstream_fallback = True
            _record_timestamp(dataset_id, stale)
            return stale
        raise IngestError(
            dataset_id,
            f"HTTP error and no local cache: {exc}. "
            "Warm the cache with network access first.",
        ) from exc
    finally:
        if own_client:
            _client.close()
