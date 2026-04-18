"""Tests for RunContext — ctx-only writes post-T7 (Story 5.3)."""

from __future__ import annotations

from pathlib import Path

import pytest

from muni_walk_access.config import load_config
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.sources.datasf import _record_timestamp
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


class TestCtxWrite:
    """Ctx-only writes post-T7: fetch-adapter helpers mutate ctx, never globals.

    T7 deleted the module-level ``_upstream_fallback`` / ``_datasf_timestamps``
    globals and their legacy setters. The remaining ``_record_timestamp``
    helper mutates ``ctx.datasf_timestamps`` directly. These tests guard the
    ctx contract: adapters with ``ctx=None`` silently no-op; adapters with
    ``ctx`` populate the run-scoped state.
    """

    @pytest.fixture
    def ctx(self, tmp_path: Path) -> RunContext:
        """Build a RunContext bound to a temp cache for ctx-write tests."""
        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        cache = CacheManager(root=tmp_path, ttl_days=1)
        return RunContext.from_config(run_id="test", config=cfg, cache=cache)

    def test_record_timestamp_without_ctx_is_noop(self) -> None:
        """``ctx=None`` (default) is a silent no-op — no globals to write to."""
        # Must not raise. Nothing to assert on: no global side-effect exists.
        _record_timestamp("abc", Path("abc-20260401.parquet"))

    def test_record_timestamp_with_ctx_populates_ctx(self, ctx: RunContext) -> None:
        """Passing ctx writes the parsed yyyymmdd suffix into ctx.datasf_timestamps."""
        assert ctx.datasf_timestamps == {}

        _record_timestamp("abc", Path("abc-20260401.parquet"), ctx=ctx)

        assert ctx.datasf_timestamps["abc"] == "20260401"
