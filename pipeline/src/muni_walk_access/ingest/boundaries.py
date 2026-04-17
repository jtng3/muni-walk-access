"""Generic boundary-source dispatch for lens geometries.

Replaces the SF-coupled `_fetch_lens_geojson` in `stratify/lens.py` with
per-source-kind adapters. `LensConfig.source_kind` drives dispatch
through `BOUNDARY_SOURCES`. Story 5.3 ships the Protocol + registry; T5a
implements `DataSFBoundarySource` for SF; ArcGIS Hub and generic-URL
impls land in Story 5.4 (Philly).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import geopandas as gpd

    from muni_walk_access.config import LensConfig
    from muni_walk_access.run_context import RunContext


class BoundarySource(Protocol):
    """Lens-boundary GeoDataFrame fetcher for one source kind.

    Implementations return a GeoDataFrame in EPSG:4326. They are
    responsible for any source-specific filtering (e.g. SF EJ score
    threshold via the generic filter engine in `_apply_lens_filter`).
    """

    def fetch(self, lens: LensConfig, ctx: RunContext) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame in EPSG:4326 for the given lens."""
        ...


BOUNDARY_SOURCES: dict[str, type[BoundarySource]] = {}


def get_boundary_source(kind: str) -> type[BoundarySource]:
    """Resolve a `BoundarySource` impl by registered source kind."""
    if kind not in BOUNDARY_SOURCES:
        raise KeyError(
            f"No BoundarySource registered for kind={kind!r}. "
            f"Known: {sorted(BOUNDARY_SOURCES)}"
        )
    return BOUNDARY_SOURCES[kind]
