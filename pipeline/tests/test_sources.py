"""Tests for AddressSource adapter registry + DataSF adapter (Story 5.3 T3)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from muni_walk_access.config import load_config
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.contracts import ResidentialAddress
from muni_walk_access.ingest.sources import (
    ADDRESS_SOURCES,
    get_address_source,
)
from muni_walk_access.ingest.sources.datasf import DataSFAddressSource
from muni_walk_access.run_context import RunContext


class TestRegistry:
    """AddressSource registry populates on package import + factory resolves."""

    def test_datasf_registered(self) -> None:
        """Importing ingest.sources triggers datasf.py → registration."""
        assert "datasf" in ADDRESS_SOURCES
        assert ADDRESS_SOURCES["datasf"] is DataSFAddressSource

    def test_factory_returns_class(self) -> None:
        """get_address_source returns the class, not an instance."""
        assert get_address_source("datasf") is DataSFAddressSource

    def test_factory_unknown_kind_raises(self) -> None:
        """Unknown kind raises KeyError with the known-kinds set."""
        with pytest.raises(KeyError, match="opa_carto"):
            get_address_source("opa_carto")


class TestDataSFAddressSourceShape:
    """Adapter output is coercible through ResidentialAddress (shape proof)."""

    def test_fetch_output_can_be_mapped_to_residential_address(
        self, tmp_path: Path
    ) -> None:
        """Per AC-5: prove one sample row is *mappable* to the 5-1 contract.

        Story-5.4 TODO: the adapter must do this rename internally so the
        DataFrame columns match ResidentialAddress fields directly. Today's
        adapter preserves EAS column names (`address`, `parcel_number`) for
        byte-identical reasons; this test documents the explicit mapping
        the next adapter (Philly OPA) will need to apply inside ``fetch``.
        """
        # Minimal mock EAS-shaped DataFrame (strings matching SODA's
        # infer_schema_length=0 output).
        mock_df = pl.DataFrame(
            {
                "address": ["1709 Broderick St"],
                "latitude": ["37.7863"],
                "longitude": ["-122.4421"],
                "parcel_number": ["0001A001"],
                "use_code": ["SRES"],
            }
        )

        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        cache = CacheManager(root=tmp_path, ttl_days=1)
        ctx = RunContext.from_config(run_id="test", config=cfg, cache=cache)

        with patch(
            "muni_walk_access.ingest.sources.datasf.fetch_residential_addresses",
            return_value=mock_df,
        ):
            source = DataSFAddressSource()
            df = source.fetch(ctx)

        assert len(df) == 1
        row = df.row(0, named=True)

        # Explicit mapping from EAS columns to ResidentialAddress fields.
        # Story 5-4 generalizes this inside the adapter; for now the mapping
        # lives in this test as documentation.
        addr = ResidentialAddress(
            address_id=row["address"],
            longitude=float(row["longitude"]),
            latitude=float(row["latitude"]),
            is_residential=True,
            use_code=row["use_code"],
            parcel_id=row["parcel_number"],
        )
        assert addr.longitude == -122.4421
        assert addr.latitude == 37.7863
        assert addr.is_residential is True

    def test_fetch_validates_coords_raises_on_out_of_range(
        self, tmp_path: Path
    ) -> None:
        """validate_wgs84 fires at the adapter boundary for bad coords."""
        bad_df = pl.DataFrame(
            {
                "address": ["fake"],
                "latitude": ["95.0"],  # > 90 — invalid WGS84
                "longitude": ["-122.4"],
                "parcel_number": ["X"],
                "use_code": ["SRES"],
            }
        )
        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        cache = CacheManager(root=tmp_path, ttl_days=1)
        ctx = RunContext.from_config(run_id="test", config=cfg, cache=cache)

        with patch(
            "muni_walk_access.ingest.sources.datasf.fetch_residential_addresses",
            return_value=bad_df,
        ):
            source = DataSFAddressSource()
            with pytest.raises(ValueError, match="latitude range"):
                source.fetch(ctx)

    def test_fetch_raises_when_all_coords_unparseable(self, tmp_path: Path) -> None:
        """All coords cast to null (e.g. SODA returns 'NA') must NOT pass.

        Regression for the bug where ``drop_nulls()`` shrunk the validation
        set to an empty series, which validate_wgs84 short-circuited as
        valid. Today the cast-with-strict=False produces an all-null Float
        series; validate_wgs84's "no non-null values" check fires.
        """
        corrupt_df = pl.DataFrame(
            {
                "address": ["a", "b"],
                "latitude": ["NA", "NA"],
                "longitude": ["bogus", "garbage"],
                "parcel_number": ["X", "Y"],
                "use_code": ["SRES", "SRES"],
            }
        )
        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        cache = CacheManager(root=tmp_path, ttl_days=1)
        ctx = RunContext.from_config(run_id="test", config=cfg, cache=cache)

        with patch(
            "muni_walk_access.ingest.sources.datasf.fetch_residential_addresses",
            return_value=corrupt_df,
        ):
            source = DataSFAddressSource()
            with pytest.raises(ValueError, match="no non-null values"):
                source.fetch(ctx)


class TestShim:
    """ingest.datasf shim aliases the canonical sources.datasf module.

    These tests guard against the binding-copy bug that `from ... import *`
    would have introduced: the legacy 9 call sites (story T3e list) must
    continue to see live state from the real module.
    """

    def test_shim_is_canonical_module(self) -> None:
        """`ingest.datasf` and `ingest.sources.datasf` are the SAME module."""
        import muni_walk_access.ingest.datasf as legacy
        import muni_walk_access.ingest.sources.datasf as canonical

        assert legacy is canonical

    def test_shim_setter_propagates_to_canonical_globals(self) -> None:
        """Calling set_upstream_fallback through the shim mutates real state."""
        import muni_walk_access.ingest.datasf as legacy_mod
        import muni_walk_access.ingest.sources.datasf as canonical_mod

        canonical_mod._upstream_fallback = False
        # Call via the shim — must update the canonical module's global,
        # not a shim-local copy.
        legacy_mod.set_upstream_fallback()
        assert canonical_mod._upstream_fallback is True
        canonical_mod._upstream_fallback = False
