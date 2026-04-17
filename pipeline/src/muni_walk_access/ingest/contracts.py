"""Pydantic input contracts for city-adapter boundaries (Story 5.1).

Mirrors the rigor of :mod:`muni_walk_access.emit.schemas` on the *output* side.
Every city's :class:`AddressSource` (Story 5.3) must return rows shaped as
:class:`ResidentialAddress`; every city's :class:`GTFSSource` returns a
:class:`GTFSFeed`. Adapter implementations validate at the boundary so
downstream code can trust the shape.
"""

from __future__ import annotations

import polars as pl
from pydantic import BaseModel, ConfigDict, field_validator


class ResidentialAddress(BaseModel):
    """Canonical residential-address row shape returned by any city adapter.

    Not used per-row in bulk fetches (a :class:`polars.DataFrame` conforming
    to these columns is the bulk-transport format). Serves as the
    single-row validator + documentation of the contract.
    """

    address_id: str
    longitude: float
    latitude: float
    is_residential: bool
    use_code: str | None = None
    parcel_id: str | None = None

    @field_validator("address_id", mode="after")
    @classmethod
    def address_id_non_empty(cls, v: str) -> str:
        """Reject empty address_id — every row must have a stable identifier."""
        if not v:
            raise ValueError("address_id must be non-empty")
        return v

    @field_validator("longitude", mode="after")
    @classmethod
    def longitude_in_wgs84_range(cls, v: float) -> float:
        """Enforce WGS84 longitude range [-180, 180]."""
        if not (-180.0 <= v <= 180.0):
            raise ValueError(f"longitude {v} outside WGS84 range [-180, 180]")
        return v

    @field_validator("latitude", mode="after")
    @classmethod
    def latitude_in_wgs84_range(cls, v: float) -> float:
        """Enforce WGS84 latitude range [-90, 90]."""
        if not (-90.0 <= v <= 90.0):
            raise ValueError(f"latitude {v} outside WGS84 range [-90, 90]")
        return v


class GTFSFeed(BaseModel):
    """Canonical GTFS feed shape returned by any city's GTFSSource adapter.

    Holds the *raw* GTFS tables downstream frequency logic needs. Per-table
    column validation is intentionally not enforced here — that belongs in
    the frequency-computation layer where required columns are known. This
    contract locks the *table set* and the feed-provenance fields.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    trips_df: pl.DataFrame
    stop_times_df: pl.DataFrame
    stops_df: pl.DataFrame
    routes_df: pl.DataFrame
    calendar_df: pl.DataFrame | None = None
    calendar_dates_df: pl.DataFrame | None = None
    feed_info_df: pl.DataFrame | None = None
    feed_sha256: str
    feed_date: str


def validate_wgs84(lats: pl.Series, lons: pl.Series) -> None:
    """Raise ValueError if any coordinate falls outside WGS84 bounds.

    Pure assertion — returns None on success. Adapters call this on full
    columns at the boundary before passing a :class:`polars.DataFrame` to
    downstream code. Single-row validation is handled by
    :class:`ResidentialAddress` field validators.

    Args:
        lats: Latitude series (expected range [-90, 90]).
        lons: Longitude series (expected range [-180, 180]).

    Raises:
        ValueError: If any latitude or longitude is outside its WGS84 range,
            with a message identifying which bound failed.

    """
    if len(lats) == 0 and len(lons) == 0:
        return

    if lats.dtype.is_float() and lats.is_nan().any():
        raise ValueError("latitude series contains NaN values")
    if lons.dtype.is_float() and lons.is_nan().any():
        raise ValueError("longitude series contains NaN values")

    lat_min, lat_max = lats.min(), lats.max()
    lon_min, lon_max = lons.min(), lons.max()

    if lat_min is None or lat_max is None:
        raise ValueError("latitude series contains no non-null values")
    if lon_min is None or lon_max is None:
        raise ValueError("longitude series contains no non-null values")

    if lat_min < -90.0 or lat_max > 90.0:
        raise ValueError(
            f"latitude range [{lat_min}, {lat_max}] outside WGS84 [-90, 90]"
        )
    if lon_min < -180.0 or lon_max > 180.0:
        raise ValueError(
            f"longitude range [{lon_min}, {lon_max}] outside WGS84 [-180, 180]"
        )
