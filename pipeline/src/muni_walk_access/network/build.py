"""Pandana pedestrian network builder with HDF5 cache."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import osmnx
import pandana

from muni_walk_access.config import Config
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.osm import fetch_osm_graph

if TYPE_CHECKING:
    from muni_walk_access.run_context import RunContext

logger = logging.getLogger(__name__)

_PANDANA_SUBDIR = "pandana"
_PANDANA_EXT: tuple[str, ...] = ("h5",)


def build_network(
    config: Config, ctx: RunContext | None = None
) -> tuple[pandana.Network, str]:
    """Build the SF pedestrian pandana Network, cache as HDF5, return (net, date).

    The pandana cache key embeds the OSM extract date so stale-fallback graphs
    produce correctly-keyed cache entries.  Returns
    ``(pandana_network, osm_extract_date_str)`` where the date is YYYYMMDD.
    """
    cache = CacheManager(
        root=config.ingest.cache_dir,
        ttl_days=config.ingest.cache_ttl_days,
    )

    # Fetch (or load from cache) the raw OSMnx graph.
    osm_graph, osm_date = fetch_osm_graph(config, cache=cache, ctx=ctx)

    pandana_dataset_id = f"pandana-contracted-{osm_date}"

    # --- Pandana cache hit ---
    pandana_cache = cache.get(
        _PANDANA_SUBDIR, pandana_dataset_id, extensions=_PANDANA_EXT
    )
    if pandana_cache is not None:
        logger.info("Pandana cache hit: %s", pandana_cache)
        net: pandana.Network = pandana.Network.from_hdf5(str(pandana_cache))
        return net, osm_date

    # --- Build pandana Network from OSMnx graph ---
    logger.info("Building pandana Network from OSMnx graph (osm_date=%s)…", osm_date)

    # Deduplicate directed edges before extracting for pandana.
    undirected = osmnx.convert.to_undirected(osm_graph)
    nodes, edges = osmnx.graph_to_gdfs(undirected)

    # In osmnx 2.x, u/v are part of the MultiIndex (u, v, key); reset to columns.
    edges_df = edges.reset_index()

    net = pandana.Network(
        node_x=nodes["x"],
        node_y=nodes["y"],
        edge_from=edges_df["u"],
        edge_to=edges_df["v"],
        edge_weights=edges_df[["length"]],
        twoway=True,
    )

    dest = cache.put_path(_PANDANA_SUBDIR, pandana_dataset_id, "h5")
    try:
        net.save_hdf5(str(dest))
    except Exception:
        dest.unlink(missing_ok=True)
        raise
    logger.info("Pandana network cached to %s", dest)

    return net, osm_date
