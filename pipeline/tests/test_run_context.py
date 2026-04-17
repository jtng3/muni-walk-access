"""Tests for RunContext + the dual-write contract (Story 5.3 T2)."""

from __future__ import annotations

from pathlib import Path

import pytest

import muni_walk_access.ingest.datasf as datasf_mod
from muni_walk_access.config import load_config
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import _record_timestamp, set_upstream_fallback
from muni_walk_access.run_context import RunContext, slugify_place


class TestSlugifyPlace:
    """slugify_place derives city_id from osm_place strings."""

    def test_sf(self) -> None:
        """SF osm_place slugifies to the expected city_id."""
        assert slugify_place("San Francisco, California, USA") == (
            "san-francisco-california-usa"
        )

    def test_philly(self) -> None:
        """Philly osm_place slugifies to the expected city_id."""
        assert slugify_place("Philadelphia, Pennsylvania, USA") == (
            "philadelphia-pennsylvania-usa"
        )

    def test_non_ascii_nfkd_normalized(self) -> None:
        """NFKD normalization strips diacritics before slugifying."""
        assert slugify_place("São Paulo, Brazil") == "sao-paulo-brazil"

    def test_empty_string_raises(self) -> None:
        """Empty input must raise — a blank city_id corrupts cache paths."""
        with pytest.raises(ValueError, match="empty slug"):
            slugify_place("")

    def test_all_whitespace_raises(self) -> None:
        """Blank slug would silently corrupt cache paths."""
        with pytest.raises(ValueError, match="empty slug"):
            slugify_place("   ")


class TestRunContextFromConfig:
    """RunContext.from_config derives city_id from the config's osm_place."""

    def test_derives_city_id(self, tmp_path: Path) -> None:
        """RunContext.from_config slugifies osm_place into city_id."""
        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        cache = CacheManager(root=tmp_path, ttl_days=1)
        ctx = RunContext.from_config(run_id="test-run", config=cfg, cache=cache)
        assert ctx.city_id == "san-francisco-california-usa"
        assert ctx.run_id == "test-run"
        assert ctx.upstream_fallback is False
        assert ctx.datasf_timestamps == {}


class TestDualWrite:
    """set_upstream_fallback and _record_timestamp must dual-write to ctx.

    Without these assertions, T7 could delete the legacy globals while ctx
    silently received nothing — byte-identical pipeline output alone doesn't
    prove the ctx path works.
    """

    @pytest.fixture
    def ctx(self, tmp_path: Path) -> RunContext:
        """Build a RunContext bound to a temp cache for setter dual-write tests."""
        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        cache = CacheManager(root=tmp_path, ttl_days=1)
        return RunContext.from_config(run_id="test", config=cfg, cache=cache)

    def test_set_upstream_fallback_without_ctx_only_writes_global(self) -> None:
        """Legacy call shape (no ctx) still works — backward compat."""
        # Reset global state
        datasf_mod._upstream_fallback = False
        set_upstream_fallback()
        assert datasf_mod._upstream_fallback is True
        # Reset
        datasf_mod._upstream_fallback = False

    def test_set_upstream_fallback_with_ctx_dual_writes(self, ctx: RunContext) -> None:
        """When ctx is supplied, both the global AND ctx must flip to True."""
        datasf_mod._upstream_fallback = False
        assert ctx.upstream_fallback is False

        set_upstream_fallback(ctx)

        assert datasf_mod._upstream_fallback is True
        assert ctx.upstream_fallback is True

        datasf_mod._upstream_fallback = False

    def test_record_timestamp_without_ctx_only_writes_global(self) -> None:
        """Legacy call shape still populates the module dict."""
        datasf_mod._datasf_timestamps.clear()
        _record_timestamp("abc", Path("abc-20260401.parquet"))
        assert datasf_mod._datasf_timestamps["abc"] == "20260401"
        datasf_mod._datasf_timestamps.clear()

    def test_record_timestamp_with_ctx_dual_writes(self, ctx: RunContext) -> None:
        """When ctx is supplied, both the module dict AND ctx must gain the key."""
        datasf_mod._datasf_timestamps.clear()
        assert ctx.datasf_timestamps == {}

        _record_timestamp("abc", Path("abc-20260401.parquet"), ctx=ctx)

        assert datasf_mod._datasf_timestamps["abc"] == "20260401"
        assert ctx.datasf_timestamps["abc"] == "20260401"

        datasf_mod._datasf_timestamps.clear()
