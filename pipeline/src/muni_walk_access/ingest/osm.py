"""OSMnx pedestrian graph fetcher with filesystem cache."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import networkx as nx
import osmnx

from muni_walk_access.config import Config
from muni_walk_access.exceptions import NetworkBuildError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import set_upstream_fallback

if TYPE_CHECKING:
    from muni_walk_access.run_context import RunContext

logger = logging.getLogger(__name__)

_OSM_SUBDIR = "osm"
_OSM_DATASET_ID = "osm-sf-pedestrian"
_OSM_EXT: tuple[str, ...] = ("graphml",)


def _date_from_cache_path(path: Path) -> str:
    """Extract the YYYYMMDD date string from a cache filename stem.

    Raises ``ValueError`` if the filename doesn't contain a valid date suffix
    rather than silently returning today's date (provenance must be accurate).
    """
    stem = path.stem  # e.g. "osm-sf-pedestrian-20260413"
    parts = stem.rsplit("-", 1)
    if len(parts) == 2:
        candidate = parts[1]
        if len(candidate) == 8 and candidate.isdigit():
            # Validate it's a real date (not e.g. "99991399")
            try:
                date(int(candidate[:4]), int(candidate[4:6]), int(candidate[6:8]))
            except ValueError:
                pass
            else:
                return candidate
    msg = f"Cannot extract YYYYMMDD date from cache filename: {path.name}"
    raise ValueError(msg)


def fetch_osm_graph(
    config: Config,
    cache: CacheManager | None = None,
    ctx: RunContext | None = None,
) -> tuple[nx.MultiDiGraph, str]:
    """Fetch the SF pedestrian network, cache as GraphML, return (graph, date).

    Returns a tuple of (osmnx_graph, osm_extract_date_str) where the date
    string is YYYYMMDD.  Uses the CacheManager for TTL-aware caching.  On
    Overpass API failure, falls back to a stale cached graph if one exists.
    Raises NetworkBuildError if neither a fresh fetch nor a stale cache is
    available.
    """
    if cache is None:
        cache = CacheManager(
            root=config.ingest.cache_dir,
            ttl_days=config.ingest.cache_ttl_days,
        )

    # --- Cache hit (fresh) ---
    fresh = cache.get(_OSM_SUBDIR, _OSM_DATASET_ID, extensions=_OSM_EXT)
    if fresh is not None:
        logger.info("OSM cache hit: %s", fresh)
        graph: nx.MultiDiGraph = osmnx.load_graphml(fresh)
        return graph, _date_from_cache_path(fresh)

    # --- Cache miss: fetch from Overpass ---
    # Narrow try scope: only the network fetch is caught broadly.
    # osmnx has no stable exception hierarchy (ValueError subclasses +
    # requests.* errors), so `except Exception` is justified here but must
    # NOT cover disk writes — those are local errors, not upstream failures.
    try:
        logger.info(
            "Fetching OSMnx pedestrian network for %s …", config.networks.osm_place
        )
        graph = osmnx.graph_from_place(
            config.networks.osm_place,
            network_type=config.networks.osm_network_type,
        )
    except Exception as exc:  # noqa: BLE001
        stale = cache.get_any(_OSM_SUBDIR, _OSM_DATASET_ID, extensions=_OSM_EXT)
        if stale is not None:
            logger.warning(
                "Overpass fetch failed (%s); using stale cache %s", exc, stale
            )
            set_upstream_fallback(ctx)
            stale_graph: nx.MultiDiGraph = osmnx.load_graphml(stale)
            return stale_graph, _date_from_cache_path(stale)
        raise NetworkBuildError(
            f"Overpass API failed and no local cache exists: {exc}. "
            "Warm the cache with network access first."
        ) from exc

    # Disk write is outside the broad catch — errors propagate naturally.
    dest = cache.put_path(_OSM_SUBDIR, _OSM_DATASET_ID, "graphml")
    try:
        osmnx.save_graphml(graph, dest)
    except Exception:
        dest.unlink(missing_ok=True)
        raise
    osm_date = date.today().strftime("%Y%m%d")
    logger.info("OSM graph cached to %s (date=%s)", dest, osm_date)
    return graph, osm_date
