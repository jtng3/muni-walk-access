"""Tests for ingest subpackage — Story 1.4 (AC-1 through AC-5, AC-7)."""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import polars as pl
import pytest

import muni_walk_access.ingest.datasf as datasf_mod
from muni_walk_access.config import Config, IngestConfig
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import (
    fetch_geospatial,
    fetch_residential_addresses,
    fetch_tabular,
)
from muni_walk_access.ingest.gtfs import fetch_gtfs
from muni_walk_access.run_context import RunContext

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
        zf.write(FIXTURE_DIR / "stops.txt", "stops.txt")
    return buf.getvalue()


def _make_gtfs_v2_zip() -> bytes:
    """Build a GTFS zip with routes.txt for v2 (per-route) frequency tests."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(FIXTURE_DIR / "trips.txt", "trips.txt")
        zf.write(FIXTURE_DIR / "stop_times.txt", "stop_times.txt")
        zf.write(FIXTURE_DIR / "stops.txt", "stops.txt")
        zf.writestr(
            "routes.txt",
            "route_id,route_short_name,route_type\n14,14,3\n28,28,3\n49,49,3\n",
        )
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

    def test_csv_mixed_column_type_not_rejected(self, tmp_path: Path) -> None:
        """Columns with letter-suffix values like '0026T' parse OK."""
        cfg = _ingest_config(tmp_path)
        csv = b"block,lot,use_code\n0001,001,SRES\n0026T,015,MRES\n"

        def handler(req: httpx.Request) -> httpx.Response:
            if "$offset=0" in str(req.url) or "$offset" not in str(req.url):
                return httpx.Response(200, content=csv)
            return httpx.Response(200, content=b"block,lot,use_code\n")

        df = fetch_tabular("mixed-schema-dataset", cfg, client=self._client(handler))
        assert "block" in df.columns
        assert "0026T" in df["block"].to_list()

    def test_csv_na_values_treated_as_null(self, tmp_path: Path) -> None:
        """'NA' in an otherwise numeric column becomes null, not a parse error."""
        cfg = _ingest_config(tmp_path)
        csv = b"id,value,code\n1,100,A\n2,NA,B\n3,300,C\n"

        def handler(req: httpx.Request) -> httpx.Response:
            if "$offset=0" in str(req.url) or "$offset" not in str(req.url):
                return httpx.Response(200, content=csv)
            return httpx.Response(200, content=b"id,value,code\n")

        df = fetch_tabular("na-test", cfg, client=self._client(handler))
        assert len(df) == 3
        assert df["value"].null_count() == 1

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
        """T9f: DataFrame has stop_id, trips_per_hour_peak, stop_lat, stop_lon."""
        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_zip()
        df, sha256, _feed_date = fetch_gtfs(config, client=self._client(zip_bytes))
        assert "stop_id" in df.columns
        assert "trips_per_hour_peak" in df.columns
        assert "stop_lat" in df.columns
        assert "stop_lon" in df.columns

    def test_gtfs_sha256_computed(self, tmp_path: Path) -> None:
        """T9g: SHA256 hash is correct for the zip bytes."""
        import hashlib

        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_zip()
        _, sha256, _feed_date = fetch_gtfs(config, client=self._client(zip_bytes))
        expected = hashlib.sha256(zip_bytes).hexdigest()
        assert sha256 == expected

    def test_peak_window_filters_stops(self, tmp_path: Path) -> None:
        """T9j: stops outside peak window (S004 in fixture) are excluded."""
        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_zip()
        df, _sha, _feed_date = fetch_gtfs(config, client=self._client(zip_bytes))
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
        df, _sha, _feed_date = fetch_gtfs(config, client=self._client(zip_bytes))
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


# ---------------------------------------------------------------------------
# Calendar correctness tests (Fix #3, audit 2026-04-16 A1)
# ---------------------------------------------------------------------------


class TestCalendarCorrectness:
    """Verify _get_active_service_ids applies date-range + calendar_dates."""

    def _make_zip_with_calendar(
        self,
        calendar_csv: str,
        calendar_dates_csv: str | None = None,
    ) -> bytes:
        """Build a GTFS zip with custom calendar files on top of the minimal fixture."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(FIXTURE_DIR / "trips.txt", "trips.txt")
            zf.write(FIXTURE_DIR / "stop_times.txt", "stop_times.txt")
            zf.write(FIXTURE_DIR / "stops.txt", "stops.txt")
            zf.writestr("calendar.txt", calendar_csv)
            if calendar_dates_csv is not None:
                zf.writestr("calendar_dates.txt", calendar_dates_csv)
        return buf.getvalue()

    def test_date_range_excludes_stale_service(self) -> None:
        """A service_id whose end_date is in the past is excluded.

        Prevents the "overlapping seasons doubles trips" bug from A1.
        """
        from muni_walk_access.ingest.gtfs import _get_active_service_ids

        # ACTIVE: Jan-Dec 2026 weekday service.
        # STALE: Jan-Jun 2020 weekday service — ended years ago.
        calendar_csv = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "ACTIVE,1,1,1,1,1,0,0,20260101,20261231\n"
            "STALE,1,1,1,1,1,0,0,20200101,20200601\n"
        )
        zf = zipfile.ZipFile(io.BytesIO(self._make_zip_with_calendar(calendar_csv)))
        result = _get_active_service_ids(zf, "weekday")
        assert result == {"ACTIVE"}
        assert "STALE" not in result

    def test_calendar_dates_removes_service(self) -> None:
        """exception_type=2 in calendar_dates.txt removes service on that date."""
        from muni_walk_access.ingest.gtfs import (
            _get_active_service_ids,
            _pick_reference_date,
        )

        # Wide validity window so ref_date lands mid-window.
        calendar_csv = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20260101,20261231\n"
        )
        ref = _pick_reference_date(
            pl.read_csv(
                io.BytesIO(calendar_csv.encode()),
                infer_schema_length=0,
            ),
            "weekday",
        )
        ref_str = ref.strftime("%Y%m%d")
        calendar_dates_csv = f"service_id,date,exception_type\nWD,{ref_str},2\n"
        zf = zipfile.ZipFile(
            io.BytesIO(self._make_zip_with_calendar(calendar_csv, calendar_dates_csv))
        )
        result = _get_active_service_ids(zf, "weekday")
        assert result == set(), (
            f"WD should be excluded by exception_type=2 on {ref_str}"
        )

    def test_calendar_dates_adds_service(self) -> None:
        """exception_type=1 adds a service_id that's not in calendar.txt."""
        from muni_walk_access.ingest.gtfs import (
            _get_active_service_ids,
            _pick_reference_date,
        )

        calendar_csv = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20260101,20261231\n"
        )
        ref = _pick_reference_date(
            pl.read_csv(io.BytesIO(calendar_csv.encode()), infer_schema_length=0),
            "weekday",
        )
        ref_str = ref.strftime("%Y%m%d")
        calendar_dates_csv = f"service_id,date,exception_type\nSPECIAL,{ref_str},1\n"
        zf = zipfile.ZipFile(
            io.BytesIO(self._make_zip_with_calendar(calendar_csv, calendar_dates_csv))
        )
        result = _get_active_service_ids(zf, "weekday")
        assert result == {"WD", "SPECIAL"}

    def test_no_calendar_files_returns_empty(self) -> None:
        """Both calendar files absent → empty set (backward-compat)."""
        from muni_walk_access.ingest.gtfs import _get_active_service_ids

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(FIXTURE_DIR / "trips.txt", "trips.txt")
            zf.write(FIXTURE_DIR / "stop_times.txt", "stop_times.txt")
            zf.write(FIXTURE_DIR / "stops.txt", "stops.txt")
        zf2 = zipfile.ZipFile(io.BytesIO(buf.getvalue()))
        result = _get_active_service_ids(zf2, "weekday")
        assert result == set()

    def test_hardcoded_date_inclusion(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Pin ``date.today()`` to a known value and verify exceptions apply.

        Breaks the circularity in the other calendar_dates tests which use
        ``_pick_reference_date`` to compute the ref string from inside the test.
        With today = 2026-06-17 (a Wednesday), the picker returns 2026-06-17
        directly (today is inside the window, no DOW shift needed).
        """
        from muni_walk_access.ingest import gtfs as gtfs_mod
        from muni_walk_access.ingest.gtfs import _get_active_service_ids

        class _FixedDate(date):
            @classmethod
            def today(cls) -> date:  # type: ignore[override]
                return date(2026, 6, 17)  # Wednesday

        monkeypatch.setattr(gtfs_mod, "date", _FixedDate)

        calendar_csv = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20260101,20261231\n"
        )
        # Exception removes WD on exactly 2026-06-17 (a holiday).
        calendar_dates_csv = (
            "service_id,date,exception_type\nWD,20260617,2\nHOLIDAY_WD,20260617,1\n"
        )
        zf = zipfile.ZipFile(
            io.BytesIO(self._make_zip_with_calendar(calendar_csv, calendar_dates_csv))
        )
        result = _get_active_service_ids(zf, "weekday")
        # WD should be removed by exception_type=2, HOLIDAY_WD added by type=1.
        assert result == {"HOLIDAY_WD"}


# ---------------------------------------------------------------------------
# fetch_residential_addresses tests (Story 1.6: T4a-T4f)
# ---------------------------------------------------------------------------


class TestResidentialFilter:
    """Tests for datasf.fetch_residential_addresses — Story 1.6."""

    def setup_method(self) -> None:
        """Reset module-level state before each test."""
        _reset_fallback()

    def _full_config(self, tmp_path: Path, parcel_id: str | None = None) -> Config:
        """Build a valid Config with tmp cache dir and optional parcel_id override."""
        from muni_walk_access.config import load_config

        config_path = Path(__file__).parent.parent / "config.yaml"
        cfg = load_config(config_path)
        updates: dict[str, Any] = {
            "ingest": IngestConfig(cache_dir=tmp_path, cache_ttl_days=30)
        }
        if parcel_id is not None:
            updates["residential_filter"] = cfg.residential_filter.model_copy(
                update={"parcel_dataset_id": parcel_id}
            )
        return cfg.model_copy(update=updates)

    def _eas_df(self) -> pl.DataFrame:
        """Minimal EAS DataFrame with parcel_number join key."""
        return pl.DataFrame(
            {
                "parcel_number": ["0001/001", "0001/002", "0002/001", "0003/001"],
                "address": ["100 MAIN ST", "200 MAIN ST", "300 OAK ST", "400 ELM ST"],
                "latitude": [37.77, 37.78, 37.79, 37.80],
                "longitude": [-122.40, -122.41, -122.42, -122.43],
            }
        )

    def _parcel_df(self) -> pl.DataFrame:
        """Multi-year parcel data — only 2024 rows should survive year filtering."""
        return pl.DataFrame(
            {
                "parcel_number": ["0001/001", "0001/002", "0002/001", "0001/001"],
                "use_code": ["SRES", "MRES", "COMM", "SRES"],
                "closed_roll_year": [2024, 2024, 2024, 2023],
            }
        )

    def _mock_fetch(
        self,
        eas_df: pl.DataFrame,
        parcel_df: pl.DataFrame,
        call_log: list[str] | None = None,
    ) -> Any:
        """Return a mock fetch_tabular that distinguishes datasets by ID."""

        def _fetch(
            dataset_id: str,
            config: IngestConfig,
            client: httpx.Client | None = None,
            ctx: RunContext | None = None,
        ) -> pl.DataFrame:
            if call_log is not None:
                call_log.append(dataset_id)
            return eas_df if dataset_id == datasf_mod._EAS_DATASET_ID else parcel_df

        return _fetch

    def test_tbd_sentinel_uses_interim_dataset(self, tmp_path: Path) -> None:
        """T4a: TBD sentinel → fetch_tabular called with _INTERIM_PARCEL_DATASET_ID."""
        cfg = self._full_config(tmp_path)
        call_log: list[str] = []

        mock = self._mock_fetch(self._eas_df(), self._parcel_df(), call_log)
        with patch.object(datasf_mod, "fetch_tabular", side_effect=mock):
            fetch_residential_addresses(cfg)

        assert datasf_mod._INTERIM_PARCEL_DATASET_ID in call_log
        assert datasf_mod._TBD_SENTINEL not in call_log

    def test_real_dataset_id_uses_configured_id(self, tmp_path: Path) -> None:
        """T4b: real parcel_dataset_id → fetch_tabular called with that ID."""
        real_id = "abcd-efgh"
        cfg = self._full_config(tmp_path, parcel_id=real_id)
        call_log: list[str] = []
        parcel_df = pl.DataFrame({"parcel_number": ["0001/001"], "use_code": ["SRES"]})

        with patch.object(
            datasf_mod,
            "fetch_tabular",
            side_effect=self._mock_fetch(self._eas_df(), parcel_df, call_log),
        ):
            fetch_residential_addresses(cfg)

        assert real_id in call_log
        assert datasf_mod._INTERIM_PARCEL_DATASET_ID not in call_log

    def test_warning_logged_for_tbd_sentinel(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T4c: WARNING level log emitted when TBD sentinel is used."""
        cfg = self._full_config(tmp_path)

        with patch.object(
            datasf_mod,
            "fetch_tabular",
            side_effect=self._mock_fetch(self._eas_df(), self._parcel_df()),
        ):
            logger_name = "muni_walk_access.ingest.datasf"
            with caplog.at_level(logging.WARNING, logger=logger_name):
                fetch_residential_addresses(cfg)

        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(datasf_mod._TBD_SENTINEL in msg for msg in warnings)

    def test_use_code_filtering(self, tmp_path: Path) -> None:
        """T4d: only rows matching use_codes_residential are returned."""
        cfg = self._full_config(tmp_path)

        with patch.object(
            datasf_mod,
            "fetch_tabular",
            side_effect=self._mock_fetch(self._eas_df(), self._parcel_df()),
        ):
            result = fetch_residential_addresses(cfg)

        # SRES + MRES match config; COMM does not; 2023 dupe excluded by year filter
        assert set(result["use_code"].to_list()).issubset({"SRES", "MRES"})
        assert len(result) == 2  # 0001/001 (SRES 2024) + 0001/002 (MRES 2024)

    def test_returned_df_has_expected_columns(self, tmp_path: Path) -> None:
        """T4e: result has address, latitude, longitude, parcel_number, use_code."""
        cfg = self._full_config(tmp_path)

        with patch.object(
            datasf_mod,
            "fetch_tabular",
            side_effect=self._mock_fetch(self._eas_df(), self._parcel_df()),
        ):
            result = fetch_residential_addresses(cfg)

        for col in ("address", "latitude", "longitude", "parcel_number", "use_code"):
            assert col in result.columns, f"Expected column missing: {col}"

    def test_ingest_error_propagates(self, tmp_path: Path) -> None:
        """T4f: IngestError from parcel fetch_tabular propagates (not swallowed)."""
        cfg = self._full_config(tmp_path)

        def failing_fetch(
            dataset_id: str,
            config: IngestConfig,
            client: httpx.Client | None = None,
            ctx: RunContext | None = None,
        ) -> pl.DataFrame:
            if dataset_id == datasf_mod._EAS_DATASET_ID:
                return self._eas_df()
            raise IngestError(
                dataset_id, "HTTP error and no local cache: upstream down"
            )

        with patch.object(datasf_mod, "fetch_tabular", side_effect=failing_fetch):
            with pytest.raises(IngestError):
                fetch_residential_addresses(cfg)


# ---------------------------------------------------------------------------
# fetch_gtfs_feed + compute_frequencies split (Story 5.3 T4 / AC-5)
# ---------------------------------------------------------------------------


class TestFetchGtfsFeedAndComputeFrequencies:
    """Covers the AC-5 restructure of fetch_gtfs_v2 → GTFSFeed contract."""

    def _full_config(self, tmp_path: Path) -> Config:
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

    def test_fetch_gtfs_feed_returns_contract_shaped_feed(self, tmp_path: Path) -> None:
        """fetch_gtfs_feed output conforms to the GTFSFeed contract."""
        import hashlib

        from muni_walk_access.ingest.contracts import GTFSFeed
        from muni_walk_access.ingest.gtfs import fetch_gtfs_feed

        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_v2_zip()
        feed = fetch_gtfs_feed(config, client=self._client(zip_bytes))

        assert isinstance(feed, GTFSFeed)
        assert feed.feed_sha256 == hashlib.sha256(zip_bytes).hexdigest()
        # Required tables are present and parsed as DataFrames.
        assert len(feed.trips_df) > 0
        assert len(feed.stop_times_df) > 0
        assert len(feed.stops_df) > 0
        assert len(feed.routes_df) > 0
        # Optional calendar tables missing in this fixture → None.
        assert feed.calendar_df is None
        assert feed.calendar_dates_df is None

    def test_compute_frequencies_produces_detail_and_summary(
        self, tmp_path: Path
    ) -> None:
        """compute_frequencies(feed, config) returns populated detail+summary."""
        from muni_walk_access.ingest.gtfs import compute_frequencies, fetch_gtfs_feed

        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_v2_zip()
        feed = fetch_gtfs_feed(config, client=self._client(zip_bytes))
        detail, summary = compute_frequencies(feed, config)

        expected_detail_cols = {
            "stop_id",
            "route_id",
            "route_short_name",
            "time_window",
            "trips_per_hour",
            "stop_lat",
            "stop_lon",
        }
        expected_summary_cols = {
            "stop_id",
            "time_window",
            "total_trips_per_hour",
            "route_count",
            "best_route_headway_min",
            "stop_lat",
            "stop_lon",
        }
        assert expected_detail_cols.issubset(set(detail.columns))
        assert expected_summary_cols.issubset(set(summary.columns))
        assert len(detail) > 0
        assert len(summary) > 0

    def test_compute_frequencies_uses_parsed_cache(self, tmp_path: Path) -> None:
        """Second compute_frequencies call hits the parsed parquet cache."""
        from muni_walk_access.ingest.gtfs import compute_frequencies, fetch_gtfs_feed

        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_v2_zip()
        feed = fetch_gtfs_feed(config, client=self._client(zip_bytes))

        detail_a, summary_a = compute_frequencies(feed, config)
        detail_b, summary_b = compute_frequencies(feed, config)

        assert detail_a.shape == detail_b.shape
        assert summary_a.shape == summary_b.shape

    def test_fetch_gtfs_feed_dataset_id_drives_meta_filename(
        self, tmp_path: Path
    ) -> None:
        """dataset_id kwarg controls the cache meta filename (AC-6 T4c)."""
        import json

        from muni_walk_access.ingest.gtfs import CACHE_SUBDIR, fetch_gtfs_feed

        config = self._full_config(tmp_path)
        zip_bytes = _make_gtfs_v2_zip()

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=zip_bytes,
                headers={
                    "last-modified": "Wed, 01 Jan 2026 00:00:00 GMT",
                    "etag": '"abc123"',
                },
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        fetch_gtfs_feed(config, client=client, dataset_id="septa-bus")

        meta_path = tmp_path / CACHE_SUBDIR / "septa-bus-http.json"
        assert meta_path.exists(), (
            f"meta filename not derived from dataset_id: {meta_path} missing"
        )
        assert json.loads(meta_path.read_text()) == {
            "etag": '"abc123"',
            "last_modified": "Wed, 01 Jan 2026 00:00:00 GMT",
        }
        zip_files = list((tmp_path / CACHE_SUBDIR).glob("septa-bus-zip-*.zip"))
        assert zip_files, "Expected zip cache under dataset_id prefix"

    def test_parse_zip_error_carries_dataset_id(self, tmp_path: Path) -> None:
        """Malformed zip surfaces IngestError tagged with adapter dataset_id."""
        from muni_walk_access.ingest.gtfs import fetch_gtfs_feed

        config = self._full_config(tmp_path)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"not a real zip")

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(IngestError) as excinfo:
            fetch_gtfs_feed(config, client=client, dataset_id="septa-bus")
        assert excinfo.value.dataset_id == "septa-bus"

    def test_304_without_cache_raises_distinct_error(self, tmp_path: Path) -> None:
        """304 without a cached zip reports drift, not upstream failure."""
        import json

        from muni_walk_access.ingest.gtfs import CACHE_SUBDIR, fetch_gtfs_feed

        config = self._full_config(tmp_path)
        # Seed a meta file so the client sends conditional headers.
        meta_path = tmp_path / CACHE_SUBDIR / "muni-gtfs-http.json"
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(
            json.dumps(
                {"etag": '"x"', "last_modified": "Wed, 01 Jan 2026 00:00:00 GMT"}
            )
        )

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(304, content=b"")

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(IngestError) as excinfo:
            fetch_gtfs_feed(config, client=client)
        assert "304 Not Modified" in str(excinfo.value)
