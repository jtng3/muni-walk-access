"""City-adapter address sources.

`AddressSource` is the Protocol every city's residential-address fetcher
implements; `ADDRESS_SOURCES` is the kind→impl registry that
`__main__.py` resolves through the factory. Story 5.3 defines the
Protocol; T3 lands the first impl (`DataSFAddressSource`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import polars as pl

    from muni_walk_access.run_context import RunContext


class AddressSource(Protocol):
    """Residential-address fetcher for one city's data source.

    Implementations return a Polars DataFrame conforming to
    :class:`muni_walk_access.ingest.contracts.ResidentialAddress` columns.
    Validation happens at the boundary inside `fetch()` via
    :func:`muni_walk_access.ingest.contracts.validate_wgs84`.
    """

    def fetch(self, ctx: RunContext) -> pl.DataFrame:
        """Return a DataFrame of residential addresses in EPSG:4326."""
        ...


ADDRESS_SOURCES: dict[str, type[AddressSource]] = {}


def get_address_source(kind: str) -> type[AddressSource]:
    """Resolve an `AddressSource` impl by registered kind ID."""
    if kind not in ADDRESS_SOURCES:
        raise KeyError(
            f"No AddressSource registered for kind={kind!r}. "
            f"Known: {sorted(ADDRESS_SOURCES)}"
        )
    return ADDRESS_SOURCES[kind]
