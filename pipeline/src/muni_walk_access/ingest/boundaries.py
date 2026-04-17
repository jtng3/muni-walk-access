"""Generic boundary-source dispatch for lens geometries.

Replaces the SF-coupled `_fetch_lens_geojson` in `stratify/lens.py` with
per-source-kind adapters. `LensConfig.source_kind` drives dispatch
through `BOUNDARY_SOURCES`. Story 5.3 T5a ships `DataSFBoundarySource`
(in `ingest/sources/datasf.py`); ArcGIS Hub and generic-URL impls are
registered here as `NotImplementedError` stubs — full impls land in
Story 5.4 (Philly OPA / PennEnviroScreen).

Also houses `_apply_lens_filter` — the generic attribute-filter engine
(AC-3) that replaces the SF-specific `if lens.id == "ej_communities":`
branch formerly at `stratify/lens.py:117`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    import geopandas as gpd

    from muni_walk_access.config import LensConfig
    from muni_walk_access.run_context import RunContext

logger = logging.getLogger(__name__)


class BoundarySource(Protocol):
    """Lens-boundary GeoDataFrame fetcher for one source kind.

    Implementations return a GeoDataFrame in EPSG:4326 (CRS-normalized,
    unfiltered). Attribute filtering is NOT the adapter's job — the
    generic :func:`_apply_lens_filter` engine below runs on the returned
    gdf in `stratify/lens.py`, so every source kind shares the same
    filter semantics.
    """

    def fetch(
        self, lens: LensConfig, ctx: RunContext | None = None
    ) -> gpd.GeoDataFrame:
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


class ArcGISHubBoundarySource:
    """BoundarySource stub for ArcGIS Hub-hosted lens geometries.

    Registered now so `LensConfig.source_kind="arcgis_hub"` routes fail
    LOUDLY instead of silently picking the wrong adapter. Full
    implementation lands with Story 5-4 (Philly Planning Districts are
    published via ArcGIS Hub).
    """

    def fetch(
        self, lens: LensConfig, ctx: RunContext | None = None
    ) -> gpd.GeoDataFrame:
        """Raise ``NotImplementedError`` — implemented in Story 5-4."""
        raise NotImplementedError(
            "ArcGISHubBoundarySource is implemented in Story 5-4 (Philly). "
            f"Cannot fetch lens id={lens.id!r}."
        )


class GenericURLBoundarySource:
    """BoundarySource stub for arbitrary-URL lens geometries.

    Registered now so `LensConfig.source_kind="generic_url"` routes fail
    LOUDLY instead of silently picking the wrong adapter. Full
    implementation lands with Story 5-4 (Philly PennEnviroScreen).
    """

    def fetch(
        self, lens: LensConfig, ctx: RunContext | None = None
    ) -> gpd.GeoDataFrame:
        """Raise ``NotImplementedError`` — implemented in Story 5-4."""
        raise NotImplementedError(
            "GenericURLBoundarySource is implemented in Story 5-4 (Philly). "
            f"Cannot fetch lens id={lens.id!r}."
        )


BOUNDARY_SOURCES["arcgis_hub"] = ArcGISHubBoundarySource
BOUNDARY_SOURCES["generic_url"] = GenericURLBoundarySource


def _apply_lens_filter(gdf: gpd.GeoDataFrame, lens: LensConfig) -> gpd.GeoDataFrame:
    """Apply the generic attribute-filter engine to a lens boundary gdf.

    Replaces the SF-specific `if lens.id == "ej_communities":` branch
    (formerly `stratify/lens.py:117`) with a config-driven rule set. No
    filter configured on the lens → pass-through. Filtering precedence:

    - ``score_threshold`` set → ``gdf[gdf[score_field] >= score_threshold]``
      (score column coerced via :func:`pandas.to_numeric` with ``errors="coerce"``
      to match SF's historical EJ behavior; null scores are excluded).
    - ``filter_field`` + ``filter_op`` ∈ {eq, ne, gte, lte, in} → apply the
      corresponding predicate. ``filter_op="in"`` requires
      ``filter_value`` to be a list.

    Both score and filter rules can coexist on the same lens; score is
    applied first, then the filter predicate.

    ``LensConfig`` validators already reject half-configured filters
    (field without op, orphan value, etc.) at config load time — this
    function trusts its input and does not re-validate.
    """
    if lens.score_field is not None and lens.score_threshold is not None:
        if lens.score_field not in gdf.columns:
            logger.warning(
                "Lens %s: score_field %r missing from gdf columns (%s); "
                "skipping score filter — all polygons retained",
                lens.id,
                lens.score_field,
                list(gdf.columns),
            )
        else:
            coerced = pd.to_numeric(gdf[lens.score_field], errors="coerce")
            gdf = gdf.assign(**{lens.score_field: coerced})
            before = len(gdf)
            gdf = gdf[gdf[lens.score_field] >= lens.score_threshold]
            logger.info(
                "Lens %s: score %s >= %s → %d/%d polygons retained",
                lens.id,
                lens.score_field,
                lens.score_threshold,
                len(gdf),
                before,
            )

    if lens.filter_field is not None and lens.filter_op is not None:
        if lens.filter_field not in gdf.columns:
            logger.warning(
                "Lens %s: filter_field %r missing from gdf columns (%s); "
                "skipping filter — all polygons retained",
                lens.id,
                lens.filter_field,
                list(gdf.columns),
            )
        else:
            before = len(gdf)
            col = gdf[lens.filter_field]
            value = lens.filter_value
            if lens.filter_op == "eq":
                gdf = gdf[col == value]
            elif lens.filter_op == "ne":
                gdf = gdf[col != value]
            elif lens.filter_op == "gte":
                gdf = gdf[col >= value]
            elif lens.filter_op == "lte":
                gdf = gdf[col <= value]
            elif lens.filter_op == "in":
                if not isinstance(value, list):
                    raise ValueError(
                        f"Lens {lens.id}: filter_op='in' requires filter_value "
                        f"to be a list, got {type(value).__name__}: {value!r}"
                    )
                gdf = gdf[col.isin(value)]
            else:
                raise ValueError(
                    f"Lens {lens.id}: unsupported filter_op={lens.filter_op!r}"
                )
            logger.info(
                "Lens %s: %s %s %r → %d/%d polygons retained",
                lens.id,
                lens.filter_field,
                lens.filter_op,
                value,
                len(gdf),
                before,
            )

    return gdf
