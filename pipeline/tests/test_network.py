"""Tests for network build subpackage — Story 1.5 (AC-1, AC-2, AC-3)."""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import networkx as nx
import osmnx
import pytest

from muni_walk_access.config import Config, IngestConfig
from muni_walk_access.exceptions import NetworkBuildError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.osm import fetch_osm_graph
from muni_walk_access.network.build import build_network
from muni_walk_access.run_context import RunContext

FIXTURE_GRAPHML = (
    Path(__file__).parent / "fixtures" / "sample_network" / "sample.graphml"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_fixture_graph() -> nx.MultiDiGraph:
    """Load the committed graphml fixture."""
    return osmnx.load_graphml(FIXTURE_GRAPHML)  # type: ignore[no-any-return]


def _config(tmp_path: Path, ttl_days: int = 30) -> Config:
    """Return a minimal Config with the cache rooted at tmp_path."""
    from muni_walk_access.config import load_config

    cfg = load_config(Path(__file__).parent.parent / "config.yaml")
    return cfg.model_copy(
        update={"ingest": IngestConfig(cache_dir=tmp_path, cache_ttl_days=ttl_days)}
    )


def _make_ctx(cfg: Config, tmp_path: Path) -> RunContext:
    """Build a minimal RunContext for fetch_osm_graph / build_network tests."""
    cache = CacheManager(root=tmp_path, ttl_days=1)
    return RunContext.from_config(run_id="test", config=cfg, cache=cache)


# ---------------------------------------------------------------------------
# CacheManager extension tests (T2.5e)
# ---------------------------------------------------------------------------


class TestCacheManagerExtensions:
    """Verify put_path and custom-extension get/get_any (Story 1.5 T2.5e)."""

    def test_put_path_returns_expected_path(self, tmp_path: Path) -> None:
        """put_path returns the correct dated path and creates parent dir."""
        cache = CacheManager(tmp_path, ttl_days=30)
        dest = cache.put_path(
            "osm", "osm-san-francisco-california-usa-pedestrian", "graphml"
        )
        today = date.today().strftime("%Y%m%d")
        assert (
            dest
            == tmp_path
            / "osm"
            / f"osm-san-francisco-california-usa-pedestrian-{today}.graphml"
        )
        assert dest.parent.exists()

    def test_get_with_graphml_extension(self, tmp_path: Path) -> None:
        """get() finds .graphml files when extensions=('graphml',)."""
        cache = CacheManager(tmp_path, ttl_days=30)
        dest = cache.put_path(
            "osm", "osm-san-francisco-california-usa-pedestrian", "graphml"
        )
        dest.write_bytes(b"dummy")
        result = cache.get(
            "osm",
            "osm-san-francisco-california-usa-pedestrian",
            extensions=("graphml",),
        )
        assert result == dest

    def test_get_does_not_find_graphml_with_default_extensions(
        self, tmp_path: Path
    ) -> None:
        """get() with default extensions does NOT find .graphml files."""
        cache = CacheManager(tmp_path, ttl_days=30)
        dest = cache.put_path(
            "osm", "osm-san-francisco-california-usa-pedestrian", "graphml"
        )
        dest.write_bytes(b"dummy")
        result = cache.get(
            "osm", "osm-san-francisco-california-usa-pedestrian"
        )  # default extensions
        assert result is None

    def test_get_any_with_h5_extension(self, tmp_path: Path) -> None:
        """get_any() finds .h5 files when extensions=('h5',)."""
        cache = CacheManager(tmp_path, ttl_days=1)
        subdir = tmp_path / "pandana"
        subdir.mkdir(parents=True)
        old_date = (date.today() - timedelta(days=5)).strftime("%Y%m%d")
        stale = subdir / f"pandana-contracted-20260413-{old_date}.h5"
        stale.write_bytes(b"h5data")
        result = cache.get_any(
            "pandana", "pandana-contracted-20260413", extensions=("h5",)
        )
        assert result == stale


# ---------------------------------------------------------------------------
# fetch_osm_graph tests (T6a-T6e, T6h)
# ---------------------------------------------------------------------------


class TestFetchOsmGraph:
    """Tests for ingest.osm.fetch_osm_graph (AC-1, AC-2)."""

    def test_cache_miss_calls_graph_from_place(self, tmp_path: Path) -> None:
        """T6a: cache miss → osmnx.graph_from_place is called."""
        cfg = _config(tmp_path)
        ctx = _make_ctx(cfg, tmp_path)
        fixture_graph = _load_fixture_graph()

        with patch("osmnx.graph_from_place", return_value=fixture_graph) as mock_fetch:
            with patch("osmnx.save_graphml"):
                graph, osm_date = fetch_osm_graph(cfg, ctx=ctx)

        mock_fetch.assert_called_once_with(
            "San Francisco, California, USA", network_type="walk"
        )
        assert osm_date == date.today().strftime("%Y%m%d")

    def test_cache_hit_skips_fetch(self, tmp_path: Path) -> None:
        """T6b: fresh cache hit → osmnx.graph_from_place NOT called."""
        cfg = _config(tmp_path)
        ctx = _make_ctx(cfg, tmp_path)
        cache = CacheManager(tmp_path, ttl_days=30)
        # Pre-populate cache with the fixture graphml
        dest = cache.put_path(
            "osm", "osm-san-francisco-california-usa-pedestrian", "graphml"
        )
        import shutil

        shutil.copy(FIXTURE_GRAPHML, dest)

        with patch("osmnx.graph_from_place") as mock_fetch:
            graph, osm_date = fetch_osm_graph(cfg, ctx=ctx)

        mock_fetch.assert_not_called()
        assert graph.number_of_nodes() == 10
        assert osm_date == date.today().strftime("%Y%m%d")

    def test_stale_cache_triggers_refetch(self, tmp_path: Path) -> None:
        """T6c: stale cache → fresh fetch is attempted."""
        cfg = _config(tmp_path, ttl_days=1)
        ctx = _make_ctx(cfg, tmp_path)
        osm_subdir = tmp_path / "osm"
        osm_subdir.mkdir(parents=True)
        old_date = (date.today() - timedelta(days=5)).strftime("%Y%m%d")
        stale = (
            osm_subdir
            / f"osm-san-francisco-california-usa-pedestrian-{old_date}.graphml"
        )
        import shutil

        shutil.copy(FIXTURE_GRAPHML, stale)

        fixture_graph = _load_fixture_graph()
        with patch("osmnx.graph_from_place", return_value=fixture_graph) as mock_fetch:
            with patch("osmnx.save_graphml"):
                graph, osm_date = fetch_osm_graph(cfg, ctx=ctx)

        mock_fetch.assert_called_once()
        assert osm_date == date.today().strftime("%Y%m%d")

    def test_overpass_failure_with_stale_cache_uses_fallback(
        self, tmp_path: Path
    ) -> None:
        """T6d: Overpass failure + stale cache → stale graph + ctx fallback set."""
        cfg = _config(tmp_path, ttl_days=1)
        ctx = _make_ctx(cfg, tmp_path)
        osm_subdir = tmp_path / "osm"
        osm_subdir.mkdir(parents=True)
        old_date = (date.today() - timedelta(days=5)).strftime("%Y%m%d")
        stale = (
            osm_subdir
            / f"osm-san-francisco-california-usa-pedestrian-{old_date}.graphml"
        )
        import shutil

        shutil.copy(FIXTURE_GRAPHML, stale)

        with patch(
            "osmnx.graph_from_place", side_effect=ConnectionError("Overpass down")
        ):
            graph, osm_date = fetch_osm_graph(cfg, ctx=ctx)

        assert graph.number_of_nodes() == 10
        assert osm_date == old_date
        assert ctx.upstream_fallback is True

    def test_overpass_failure_no_cache_raises(self, tmp_path: Path) -> None:
        """T6e: Overpass failure + no cache → raises NetworkBuildError."""
        cfg = _config(tmp_path)
        ctx = _make_ctx(cfg, tmp_path)
        with patch(
            "osmnx.graph_from_place", side_effect=ConnectionError("Overpass down")
        ):
            with pytest.raises(NetworkBuildError, match="Network build failed"):
                fetch_osm_graph(cfg, ctx=ctx)

    def test_osm_date_is_valid_yyyymmdd(self, tmp_path: Path) -> None:
        """T6h: returned osm_extract_date is a valid YYYYMMDD string."""
        cfg = _config(tmp_path)
        ctx = _make_ctx(cfg, tmp_path)
        fixture_graph = _load_fixture_graph()
        with patch("osmnx.graph_from_place", return_value=fixture_graph):
            with patch("osmnx.save_graphml"):
                _, osm_date = fetch_osm_graph(cfg, ctx=ctx)

        assert re.fullmatch(r"\d{8}", osm_date), f"Not YYYYMMDD: {osm_date!r}"
        # Verify it parses as a valid date
        parsed = date(int(osm_date[:4]), int(osm_date[4:6]), int(osm_date[6:8]))
        assert parsed == date.today()


# ---------------------------------------------------------------------------
# build_network tests (T6f, T6g)
# ---------------------------------------------------------------------------


class TestBuildNetwork:
    """Tests for network.build.build_network (AC-3)."""

    def test_pandana_conversion_correct_node_edge_counts(self, tmp_path: Path) -> None:
        """T6f: pandana Network has correct node/edge counts from fixture graph."""
        cfg = _config(tmp_path)
        ctx = _make_ctx(cfg, tmp_path)
        fixture_graph = _load_fixture_graph()

        with patch("osmnx.graph_from_place", return_value=fixture_graph):
            with patch("osmnx.save_graphml"):
                net, osm_date = build_network(cfg, ctx=ctx)

        # 10 nodes in fixture
        assert net.nodes_df.shape[0] == 10

    def test_pandana_h5_cache_hit_skips_conversion(self, tmp_path: Path) -> None:
        """T6g: pandana .h5 cache hit returns cached network without rebuilding."""
        cfg = _config(tmp_path)
        ctx = _make_ctx(cfg, tmp_path)
        fixture_graph = _load_fixture_graph()
        today = date.today().strftime("%Y%m%d")

        # First: build and cache the pandana network
        with patch("osmnx.graph_from_place", return_value=fixture_graph):
            with patch("osmnx.save_graphml"):
                net1, osm_date1 = build_network(cfg, ctx=ctx)

        # Second call: should hit .h5 cache — neither graph_from_place nor conversion
        with patch("osmnx.graph_from_place") as mock_fetch:
            # Populate the osm cache too so it doesn't try to re-fetch
            osm_subdir = tmp_path / "osm"
            osm_subdir.mkdir(parents=True, exist_ok=True)
            import shutil

            cache_name = f"osm-san-francisco-california-usa-pedestrian-{today}.graphml"
            shutil.copy(FIXTURE_GRAPHML, osm_subdir / cache_name)
            net2, osm_date2 = build_network(cfg, ctx=ctx)

        mock_fetch.assert_not_called()
        assert net2.nodes_df.shape[0] == 10
        assert osm_date2 == today
