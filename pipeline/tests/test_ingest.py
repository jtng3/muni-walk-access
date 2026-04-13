"""Tests for ingest subpackage — Story 1.4 (AC-1 through AC-5, AC-7)."""

from __future__ import annotations

import io
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest

import muni_walk_access.ingest.datasf as datasf_mod
from muni_walk_access.config import Config, IngestConfig
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import fetch_geospatial, fetch_tabular
from muni_walk_access.ingest.gtfs import fetch_gtfs

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sample_gtfs_minimal"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_csv_bytes(rows: int = 3) -> bytes:
    """Return minimal CSV bytes for a fake SODA response."""
    lines = ["stop_id,stop_name"] + [f"S{i},Stop {i}" for i in range(rows)]
    return "\n".join(lines).encode()


def _make_parquet_bytes(rows: int = 3) -> bytes:
    """Return parquet bytes for a small DataFrame."""
    df = pl.DataFrame(
        {
            "stop_id": [f"S{i}" for i in range(rows)],
            "stop_name": [f"Stop {i}" for i in range(rows)],
        }
    )
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def _make_gtfs_zip() -> bytes:
    """Build a GTFS zip from the minimal fixture text files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(FIXTURE_DIR / "trips.txt", "trips.txt")
        zf.write(FIXTURE_DIR / "stop_times.txt", "stop_times.txt")
    return buf.getvalue()


def _ingest_config(tmp_path: Path, ttl_days: int = 30) -> IngestConfig:
    return IngestConfig(cache_dir=tmp_path, cache_ttl_days=ttl_days)


def _reset_fallback() -> None:
    """Reset the module-level upstream_fallback flag between tests."""
    datasf_mod._upstream_fallback = False
    datasf_mod._datasf_timestamps.clear()


# ---------------------------------------------------------------------------
# CacheManager unit tests (T9a-T9c)
# ---------------------------------------------------------------------------


class TestCacheManager:
    """Unit tests for CacheManager get/put/get_any/freshness logic."""

    def test_cache_miss_returns_none(self, tmp_path: Path) -> None:
        """T9b: cache miss → get() returns None."""
        cache = CacheManager(tmp_path, ttl_days=30)
        assert cache.get("datasf", "test-dataset") is None

    def test_put_then_get_returns_fresh(self, tmp_path: Path) -> None:
        """T9a: write cache → fresh get() returns the path."""
        cache = CacheManager(tmp_path, ttl_days=30)
        data = b"hello parquet"
        cache.put("datasf", "test-dataset", data, "parquet")
        result = cache.get("datasf", "test-dataset")
        assert result is not None
        assert result.read_bytes() == data

    def test_stale_cache_get_returns_none(self, tmp_path: Path) -> None:
        """T9c: stale cache → get() returns None, get_any() returns path."""
        cache = CacheManager(tmp_path, ttl_days=1)
        # Write a file dated 10 days ago
        subdir = tmp_path / "datasf"
        subdir.mkdir(parents=True, exist_ok=True)
        old_date = (date.today() - timedelta(days=10)).strftime("%Y%m%d")
        old_file = subdir / f"my-dataset-{old_date}.parquet"
        old_file.write_bytes(b"old data")

        assert cache.get("datasf", "my-dataset") is None
        assert cache.get_any("datasf", "my-dataset") == old_file

    def test_get_any_returns_none_when_no_cache(self, tmp_path: Path) -> None:
        """get_any() returns None if no file exists at all."""
        cache = CacheManager(tmp_path, ttl_days=30)
        assert cache.get_any("datasf", "no-such-dataset") is None


# ---------------------------------------------------------------------------
# fetch_tabular tests (T9a-T9e, T9e2)
# ---------------------------------------------------------------------------


class TestFetchTabular:
    """Tests for datasf.fetch_tabular."""

    def setup_method(self) -> None:
        """Reset module-level state before each test."""
        _reset_fallback()

    def _client(self, handler: Any) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_cache_hit_no_http(self, tmp_path: Path) -> None:
        """T9a: cache hit → returns cached DataFrame without HTTP call."""
        cfg = _ingest_config(tmp_path)
        # Pre-populate cache
        cache = CacheManager(tmp_path, ttl_days=30)
        cache.put("datasf", "dataset-abc", _make_parquet_bytes(5), "parquet")

        call_count = 0

        def handler(req: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, content=_make_csv_bytes())

        client = self._client(handler)
        df = fetch_tabular("dataset-abc", cfg, client=client)
        assert call_count == 0
        assert len(df) == 5

    def test_cache_miss_fetches_and_caches(self, tmp_path: Path) -> None:
        """T9b: cache miss → HTTP fetch, result cached."""
        cfg = _ingest_config(tmp_path)

        def handler(req: httpx.Request) -> httpx.Response:
            if "offset=0" in str(req.url) or "$offset" not in str(req.url):
                return httpx.Response(200, content=_make_csv_bytes(3))
            return httpx.Response(200, content=b"stop_id,stop_name\n")  # empty page

        client = self._client(handler)
        df = fetch_tabular("dataset-xyz", cfg, client=client)
        assert len(df) == 3

        # Second call should hit cache (no HTTP)
        call_count = 0

        def no_http(req: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, content=_make_csv_bytes())

        df2 = fetch_tabular("dataset-xyz", cfg, client=self._client(no_http))
        assert call_count == 0
        assert len(df2) == 3

    def test_fallback_on_http_error_with_cache(self, tmp_path: Path) -> None:
        """T9d: HTTP error + stale cache → returns stale data, sets fallback flag."""
        cfg = _ingest_config(tmp_path, ttl_days=1)

        # Write old cache manually
        subdir = tmp_path / "datasf"
        subdir.mkdir(parents=True, exist_ok=True)
        old_date = (date.today() - timedelta(days=5)).strftime("%Y%m%d")
        old_file = subdir / f"fail-dataset-{old_date}.parquet"
        pl.DataFrame({"stop_id": ["S0", "S1"]}).write_parquet(old_file)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(500, content=b"server error")

        client = self._client(handler)
        df = fetch_tabular("fail-dataset", cfg, client=client)
        assert list(df["stop_id"]) == ["S0", "S1"]
        assert datasf_mod.was_fallback_used() is True

    def test_no_fallback_flag_on_success(self, tmp_path: Path) -> None:
        """T9e2: successful fetch → was_fallback_used() is False."""
        cfg = _ingest_config(tmp_path)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=_make_csv_bytes(2))

        fetch_tabular("ok-dataset", cfg, client=self._client(handler))
        assert datasf_mod.was_fallback_used() is False

    def test_ingest_error_on_http_failure_no_cache(self, tmp_path: Path) -> None:
        """T9e: HTTP error + no cache → raises IngestError."""
        cfg = _ingest_config(tmp_path)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(500, content=b"error")

        with pytest.raises(IngestError) as exc_info:
            fetch_tabular("no-cache-dataset", cfg, client=self._client(handler))

        assert "no-cache-dataset" in str(exc_info.value)
        assert "Warm the cache" in str(exc_info.value)

    def test_stale_cache_triggers_refetch(self, tmp_path: Path) -> None:
        """T9c: stale cache → fetch is attempted (fresh data overwrites)."""
        cfg = _ingest_config(tmp_path, ttl_days=1)

        # Write stale cache
        subdir = tmp_path / "datasf"
        subdir.mkdir(parents=True, exist_ok=True)
        old_date = (date.today() - timedelta(days=5)).strftime("%Y%m%d")
        stale = subdir / f"refresh-dataset-{old_date}.parquet"
        stale.write_bytes(_make_parquet_bytes(1))

        fetch_count = 0

        def handler(req: httpx.Request) -> httpx.Response:
            nonlocal fetch_count
            fetch_count += 1
            return httpx.Response(200, content=_make_csv_bytes(7))

        df = fetch_tabular("refresh-dataset", cfg, client=self._client(handler))
        assert fetch_count >= 1
        assert len(df) == 7


# ---------------------------------------------------------------------------
# fetch_geospatial tests
# ---------------------------------------------------------------------------


class TestFetchGeospatial:
    """Tests for datasf.fetch_geospatial."""

    def setup_method(self) -> None:
        """Reset module-level state before each test."""
        _reset_fallback()

    def _client(self, handler: Any) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_fetch_and_cache(self, tmp_path: Path) -> None:
        """Fetch geospatial dataset and verify it's cached as .geojson."""
        cfg = _ingest_config(tmp_path)
        geojson_bytes = b'{"type":"FeatureCollection","features":[]}'

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=geojson_bytes)

        path = fetch_geospatial("geo-dataset", cfg, client=self._client(handler))
        assert path.suffix == ".geojson"
        assert path.read_bytes() == geojson_bytes

    def test_cache_hit_no_http(self, tmp_path: Path) -> None:
        """Cache hit returns cached path without HTTP call."""
        cfg = _ingest_config(tmp_path)
        cache = CacheManager(tmp_path, ttl_days=30)
        cache.put("datasf", "geo-cached", b'{"type":"FeatureCollection"}', "geojson")

        call_count = 0

        def handler(req: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, content=b"{}")

        path = fetch_geospatial("geo-cached", cfg, client=self._client(handler))
        assert call_count == 0
        assert path.exists()

    def test_fallback_on_geo_failure(self, tmp_path: Path) -> None:
        """HTTP failure with stale cache → returns stale path, sets fallback."""
        cfg = _ingest_config(tmp_path, ttl_days=1)
        subdir = tmp_path / "datasf"
        subdir.mkdir(parents=True, exist_ok=True)
        old_date = (date.today() - timedelta(days=5)).strftime("%Y%m%d")
        stale_file = subdir / f"geo-fail-{old_date}.geojson"
        stale_file.write_bytes(b'{"type":"old"}')

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(500, content=b"fail")

        path = fetch_geospatial("geo-fail", cfg, client=self._client(handler))
        assert path == stale_file
        assert datasf_mod.was_fallback_used() is True

    def test_ingest_error_geo_no_cache(self, tmp_path: Path) -> None:
        """HTTP failure with no cache → raises IngestError."""
        cfg = _ingest_config(tmp_path)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(500, content=b"fail")

        with pytest.raises(IngestError) as exc_info:
            fetch_geospatial("geo-nocache", cfg, client=self._client(handler))
        assert "geo-nocache" in str(exc_info.value)


# ---------------------------------------------------------------------------
# fetch_gtfs tests (T9f, T9g, T9j)
# ---------------------------------------------------------------------------


class TestFetchGtfs:
    """Tests for ingest.gtfs.fetch_gtfs."""

    def _full_config(self, tmp_path: Path) -> Config:
        """Build a minimal but valid Config for GTFS tests."""
        from muni_walk_access.config import load_config

        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        return cfg.model_copy(
            update={"ingest": IngestConfig(cache_dir=tmp_path, cache_ttl_days=30)}
        )

    def _client(self, zip_bytes: bytes) -> httpx.Client:
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=zip_bytes)

        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_gtfs_produces_correct_columns(self, tmp_path: Path) -> None:
        """T9f: parsed DataFrame has stop_id and trips_per_hour_peak columns."""
        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_zip()
        df, sha256 = fetch_gtfs(config, client=self._client(zip_bytes))
        assert "stop_id" in df.columns
        assert "trips_per_hour_peak" in df.columns

    def test_gtfs_sha256_computed(self, tmp_path: Path) -> None:
        """T9g: SHA256 hash is correct for the zip bytes."""
        import hashlib

        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_zip()
        _, sha256 = fetch_gtfs(config, client=self._client(zip_bytes))
        expected = hashlib.sha256(zip_bytes).hexdigest()
        assert sha256 == expected

    def test_peak_window_filters_stops(self, tmp_path: Path) -> None:
        """T9j: stops outside peak window (S004 in fixture) are excluded."""
        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_zip()
        df, _ = fetch_gtfs(config, client=self._client(zip_bytes))
        stop_ids = df["stop_id"].to_list()
        # S004 is only served at 10:00+ — outside 07:00–09:00 peak
        assert "S004" not in stop_ids
        # S001, S002, S003 should be present
        assert "S001" in stop_ids
        assert "S003" in stop_ids

    def test_trips_per_hour_correct(self, tmp_path: Path) -> None:
        """S001 has 8 trips in 2hr window → 4 trips/hr."""
        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_zip()
        df, _ = fetch_gtfs(config, client=self._client(zip_bytes))
        s001 = df.filter(pl.col("stop_id") == "S001")
        assert len(s001) == 1
        assert abs(s001["trips_per_hour_peak"][0] - 4.0) < 0.01

    def test_gtfs_ingest_error_no_cache(self, tmp_path: Path) -> None:
        """All GTFS URLs fail + no cache → raises IngestError."""
        config = self._full_config(tmp_path)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(500, content=b"error")

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(IngestError):
            fetch_gtfs(config, client=client)
