"""Tests for input contracts (Story 5.1)."""

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from muni_walk_access.ingest.contracts import (
    GTFSFeed,
    ResidentialAddress,
    validate_wgs84,
)


class TestResidentialAddress:
    """Field-level validation for the single-row canonical address contract."""

    def test_sf_shape_constructs(self) -> None:
        """SF-shaped row (parcel_id from EAS) constructs cleanly."""
        addr = ResidentialAddress(
            address_id="12345",
            longitude=-122.4194,
            latitude=37.7749,
            is_residential=True,
            use_code="SRES",
            parcel_id="0123456789",
        )
        assert addr.address_id == "12345"
        assert addr.use_code == "SRES"

    def test_philly_shape_constructs(self) -> None:
        """Philly-shaped row (parcel_id from OPA) constructs cleanly."""
        addr = ResidentialAddress(
            address_id="881234567",
            longitude=-75.1652,
            latitude=39.9526,
            is_residential=True,
            use_code="1",
            parcel_id="881234567",
        )
        assert addr.latitude == pytest.approx(39.9526)

    def test_optional_fields_default_none(self) -> None:
        """`use_code` and `parcel_id` are nullable and default to None."""
        addr = ResidentialAddress(
            address_id="abc",
            longitude=0.0,
            latitude=0.0,
            is_residential=False,
        )
        assert addr.use_code is None
        assert addr.parcel_id is None

    def test_rejects_longitude_above_180(self) -> None:
        """Reject longitude above WGS84 upper bound."""
        with pytest.raises(ValidationError, match="longitude 181.0 outside"):
            ResidentialAddress(
                address_id="x",
                longitude=181.0,
                latitude=0.0,
                is_residential=True,
            )

    def test_rejects_longitude_below_neg_180(self) -> None:
        """Reject longitude below WGS84 lower bound."""
        with pytest.raises(ValidationError, match="longitude -200.0 outside"):
            ResidentialAddress(
                address_id="x",
                longitude=-200.0,
                latitude=0.0,
                is_residential=True,
            )

    def test_rejects_latitude_above_90(self) -> None:
        """Reject latitude above WGS84 upper bound."""
        with pytest.raises(ValidationError, match="latitude 91.0 outside"):
            ResidentialAddress(
                address_id="x",
                longitude=0.0,
                latitude=91.0,
                is_residential=True,
            )

    def test_rejects_latitude_below_neg_90(self) -> None:
        """Reject latitude below WGS84 lower bound."""
        with pytest.raises(ValidationError, match="latitude -91.0 outside"):
            ResidentialAddress(
                address_id="x",
                longitude=0.0,
                latitude=-91.0,
                is_residential=True,
            )

    def test_rejects_empty_address_id(self) -> None:
        """Reject empty string as address_id."""
        with pytest.raises(ValidationError, match="address_id must be non-empty"):
            ResidentialAddress(
                address_id="",
                longitude=0.0,
                latitude=0.0,
                is_residential=True,
            )

    def test_rejects_missing_is_residential(self) -> None:
        """Reject construction without the required `is_residential` field."""
        with pytest.raises(ValidationError):
            ResidentialAddress(  # type: ignore[call-arg]
                address_id="x",
                longitude=0.0,
                latitude=0.0,
            )


class TestGTFSFeed:
    """Table-set validation for the GTFS feed contract."""

    @staticmethod
    def _minimal_feed() -> GTFSFeed:
        """Build a GTFSFeed with minimal-but-valid empty DataFrames."""
        return GTFSFeed(
            trips_df=pl.DataFrame({"trip_id": [], "service_id": [], "route_id": []}),
            stop_times_df=pl.DataFrame({"trip_id": [], "stop_id": []}),
            stops_df=pl.DataFrame({"stop_id": [], "stop_lat": [], "stop_lon": []}),
            routes_df=pl.DataFrame({"route_id": []}),
            feed_sha256="a" * 64,
            feed_date="2026-04-17",
        )

    def test_minimal_feed_constructs(self) -> None:
        """A feed with only the required tables + provenance fields is valid."""
        feed = self._minimal_feed()
        assert feed.feed_sha256 == "a" * 64
        assert feed.calendar_df is None
        assert feed.calendar_dates_df is None

    def test_round_trip_preserves_non_dataframe_fields(self) -> None:
        """Verify `model_dump()` preserves provenance fields (AC-4 round-trip)."""
        feed = self._minimal_feed()
        dumped = feed.model_dump()
        assert dumped["feed_sha256"] == "a" * 64
        assert dumped["feed_date"] == "2026-04-17"
        assert dumped["calendar_df"] is None
        assert dumped["calendar_dates_df"] is None
        assert dumped["feed_info_df"] is None

    def test_optional_tables_accepted(self) -> None:
        """Accept the optional calendar, calendar_dates, and feed_info tables."""
        feed = GTFSFeed(
            trips_df=pl.DataFrame({"trip_id": []}),
            stop_times_df=pl.DataFrame({"trip_id": []}),
            stops_df=pl.DataFrame({"stop_id": []}),
            routes_df=pl.DataFrame({"route_id": []}),
            calendar_df=pl.DataFrame({"service_id": [], "monday": []}),
            calendar_dates_df=pl.DataFrame({"service_id": [], "date": []}),
            feed_info_df=pl.DataFrame({"feed_publisher_name": []}),
            feed_sha256="b" * 64,
            feed_date="",
        )
        assert feed.calendar_df is not None
        assert feed.calendar_dates_df is not None
        assert feed.feed_info_df is not None

    def test_rejects_missing_trips_df(self) -> None:
        """Reject construction without the required `trips_df` field."""
        with pytest.raises(ValidationError):
            GTFSFeed(  # type: ignore[call-arg]
                stop_times_df=pl.DataFrame(),
                stops_df=pl.DataFrame(),
                routes_df=pl.DataFrame(),
                feed_sha256="c" * 64,
                feed_date="2026-04-17",
            )

    def test_rejects_non_dataframe_table(self) -> None:
        """Reject a string (or any non-DataFrame type) for a required table field."""
        with pytest.raises(ValidationError):
            GTFSFeed(
                trips_df="not a dataframe",  # type: ignore[arg-type]
                stop_times_df=pl.DataFrame(),
                stops_df=pl.DataFrame(),
                routes_df=pl.DataFrame(),
                feed_sha256="d" * 64,
                feed_date="2026-04-17",
            )


class TestValidateWgs84:
    """Boundary assertion for full coordinate columns."""

    def test_accepts_sf_coordinates(self) -> None:
        """Pass for an SF-area bounding box."""
        lats = pl.Series([37.77, 37.80, 37.75])
        lons = pl.Series([-122.41, -122.45, -122.39])
        validate_wgs84(lats, lons)

    def test_accepts_philly_coordinates(self) -> None:
        """Pass for a Philly-area bounding box."""
        lats = pl.Series([39.95, 40.05, 39.87])
        lons = pl.Series([-75.17, -75.28, -74.96])
        validate_wgs84(lats, lons)

    def test_rejects_bad_latitude(self) -> None:
        """Raise when any latitude falls outside [-90, 90]."""
        lats = pl.Series([37.77, 91.0, 37.75])
        lons = pl.Series([-122.41, -122.45, -122.39])
        with pytest.raises(ValueError, match="latitude range"):
            validate_wgs84(lats, lons)

    def test_rejects_bad_longitude(self) -> None:
        """Raise when any longitude falls outside [-180, 180]."""
        lats = pl.Series([37.77, 37.80, 37.75])
        lons = pl.Series([-122.41, -200.0, -122.39])
        with pytest.raises(ValueError, match="longitude range"):
            validate_wgs84(lats, lons)

    def test_empty_series_is_noop(self) -> None:
        """Empty columns shouldn't crash — adapters may legitimately return 0 rows."""
        validate_wgs84(pl.Series([], dtype=pl.Float64), pl.Series([], dtype=pl.Float64))

    def test_rejects_nan_latitude(self) -> None:
        """Reject a NaN in latitudes — NaN is exactly the junk this guards against."""
        lats = pl.Series([37.77, float("nan"), 37.75])
        lons = pl.Series([-122.41, -122.45, -122.39])
        with pytest.raises(ValueError, match="latitude series contains NaN"):
            validate_wgs84(lats, lons)

    def test_rejects_nan_longitude(self) -> None:
        """Reject a NaN in longitudes."""
        lats = pl.Series([37.77, 37.80, 37.75])
        lons = pl.Series([-122.41, float("nan"), -122.39])
        with pytest.raises(ValueError, match="longitude series contains NaN"):
            validate_wgs84(lats, lons)
