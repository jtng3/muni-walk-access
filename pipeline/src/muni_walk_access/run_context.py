"""RunContext — single run-scoped state container.

Replaces the legacy module-level globals (`_upstream_fallback`,
`_datasf_timestamps`) in `ingest/datasf.py`. One `RunContext` is constructed
per pipeline invocation in `__main__.py` and threaded into every fetch
function. Story 5.3 introduces this as a dual-write target alongside the
old globals; T7 deletes the globals once every consumer reads from `ctx`.

Single-threaded by design — the pipeline runs one city at a time. When
multi-city parallelism arrives (post-5.4), each city gets its own ctx.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx

    from muni_walk_access.config import Config
    from muni_walk_access.ingest.cache import CacheManager


def slugify_place(name: str) -> str:
    """Lowercase + collapse non-alphanumerics to hyphens.

    Used to derive ``city_id`` from ``config.networks.osm_place``. Examples:
    'San Francisco, California, USA' → 'san-francisco-california-usa';
    'Philadelphia, Pennsylvania, USA' → 'philadelphia-pennsylvania-usa';
    'São Paulo, Brazil' → 'sao-paulo-brazil' (NFKD-normalized).

    Raises ValueError if the input is empty or normalizes to empty (e.g. all
    whitespace) — a blank ``city_id`` would silently corrupt cache paths.
    """
    ascii_name = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    )
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    if not slug:
        raise ValueError(f"slugify_place produced empty slug from {name!r}")
    return slug


@dataclass
class RunContext:
    """Run-scoped state + shared resources for one pipeline invocation.

    Mutable fields (`upstream_fallback`, `datasf_timestamps`) are written
    from inside fetch functions and read at emit time for provenance.
    """

    run_id: str
    config: Config
    # ``cache`` is held on the ctx for future wire-up; in T2 the fetch
    # functions still construct their own CacheManager. T3 starts reading
    # from ``ctx.cache``; until then this field is informational.
    cache: CacheManager
    city_id: str
    http_client: httpx.Client | None = None
    upstream_fallback: bool = False
    datasf_timestamps: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_config(
        cls,
        *,
        run_id: str,
        config: Config,
        cache: CacheManager,
        http_client: httpx.Client | None = None,
    ) -> RunContext:
        """Build a RunContext, deriving `city_id` from `config.networks.osm_place`."""
        return cls(
            run_id=run_id,
            config=config,
            cache=cache,
            city_id=slugify_place(config.networks.osm_place),
            http_client=http_client,
        )
