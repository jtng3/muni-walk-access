"""GeoJSON emitter for the muni-walk-access data contract."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import geopandas as gpd

from muni_walk_access.config import Config
from muni_walk_access.emit.schemas import (
    NeighborhoodFeatureProperties,
    NeighborhoodGrid,
)
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.stratify.lens import slugify_neighborhood

logger = logging.getLogger(__name__)


def _round_coord_value(value: Any, decimals: int) -> Any:
    """Recursively round coordinate values in a GeoJSON coordinate structure."""
    if isinstance(value, (int, float)):
        return round(float(value), decimals)
    if isinstance(value, (list, tuple)):
        return [_round_coord_value(v, decimals) for v in value]
    return value


def _round_coords(geometry: dict[str, Any], decimals: int = 6) -> dict[str, Any]:
    """Return a copy of a GeoJSON geometry with all coordinates rounded."""
    coords = geometry.get("coordinates")
    if coords is None:
        return geometry
    return {**geometry, "coordinates": _round_coord_value(coords, decimals)}


def write_neighborhoods_geojson(
    neighborhoods: list[NeighborhoodGrid],
    config: Config,
    output_dir: Path,
) -> Path:
    """Write neighborhoods.geojson to {output_dir}/site/public/data/.

    Loads the Analysis Neighborhoods boundary from the cache populated during the
    stratify stage (no network call). Features are sorted by properties.id and
    all coordinates are rounded to 6 decimal places.

    Returns the path to the written file.
    """
    if not neighborhoods:
        raise ValueError("neighborhoods must not be empty")

    cache = CacheManager(
        root=config.ingest.cache_dir,
        ttl_days=config.ingest.cache_ttl_days,
    )
    boundary_path = cache.get_any("datasf", config.lenses[0].datasf_id)
    if boundary_path is None:
        raise ValueError(
            f"No cached boundary data for Analysis Neighborhoods "
            f"(dataset {config.lenses[0].datasf_id})"
        )

    boundaries: gpd.GeoDataFrame = gpd.read_file(boundary_path)

    freq_idx = config.grid.frequency_threshold_min.index(
        config.grid.defaults.frequency_min
    )
    walk_idx = config.grid.walking_minutes.index(config.grid.defaults.walking_min)

    # Build slug → geometry lookup from cached boundary data
    slug_to_geom: dict[str, dict[str, Any]] = {}
    for _, row in boundaries.iterrows():
        slug = slugify_neighborhood(str(row["nhood"]))
        slug_to_geom[slug] = row.geometry.__geo_interface__

    features: list[dict[str, Any]] = []
    for nbhd in neighborhoods:
        geom = slug_to_geom.get(nbhd.id)
        if geom is None:
            logger.warning("No boundary geometry found for neighbourhood %s", nbhd.id)
            continue
        pct_at_defaults = nbhd.pct_within[freq_idx][walk_idx]
        props = NeighborhoodFeatureProperties(
            id=nbhd.id,
            name=nbhd.name,
            population=nbhd.population,
            lens_flags=nbhd.lens_flags,
            pct_at_defaults=pct_at_defaults,
        )
        features.append(
            {
                "type": "Feature",
                "geometry": _round_coords(geom),
                "properties": json.loads(props.model_dump_json()),
            }
        )

    features.sort(key=lambda f: f["properties"]["id"])
    geojson: dict[str, Any] = {"type": "FeatureCollection", "features": features}

    out_dir = output_dir / "site" / "public" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "neighborhoods.geojson"
    out_path.write_text(json.dumps(geojson, indent=2))
    logger.info("Neighborhoods GeoJSON written: %s", out_path)
    return out_path
