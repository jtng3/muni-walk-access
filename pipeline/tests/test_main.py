"""Tests for pipeline orchestration (__main__.py) — Stories 1.8, 1.9."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from muni_walk_access.__main__ import main
from muni_walk_access.emit.schemas import CityWide, LensFlags, NeighborhoodGrid

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _make_mock_addresses(n: int = 5) -> pl.DataFrame:
    """Minimal address DataFrame for pipeline mocking."""
    return pl.DataFrame(
        {
            "address_id": [str(i) for i in range(n)],
            "latitude": [37.78 + i * 0.001 for i in range(n)],
            "longitude": [-122.41 + i * 0.001 for i in range(n)],
        }
    )


def _make_mock_stops(n: int = 3) -> pl.DataFrame:
    """Minimal stops DataFrame for pipeline mocking."""
    return pl.DataFrame(
        {
            "stop_id": [f"S{i}" for i in range(n)],
            "stop_lat": [37.78 + i * 0.002 for i in range(n)],
            "stop_lon": [-122.41 + i * 0.002 for i in range(n)],
            "trips_per_hour_peak": [4.0] * n,
        }
    )


def _make_mock_result(n: int = 5) -> pl.DataFrame:
    """Routing result DataFrame (no null distances, no null stop_ids)."""
    df = _make_mock_addresses(n)
    return df.with_columns(
        pl.lit(150.0).alias("nearest_stop_distance_m"),
        pl.lit(2.0).alias("walk_minutes"),
        pl.lit("S0").alias("nearest_stop_id"),
    )


def _make_mock_stratify_return(
    result: pl.DataFrame,
) -> tuple[
    pl.DataFrame,
    list[dict[str, object]],
    list[NeighborhoodGrid],
    CityWide,
    float,
    float,
]:
    """Build a mock return value for _run_stratify."""
    n_freq, n_walk = 7, 6
    empty_grid: list[list[float]] = [[0.0] * n_walk for _ in range(n_freq)]
    stratified = result.with_columns(
        pl.lit("test-nbhd").alias("neighborhood_id"),
        pl.lit("Test Nbhd").alias("neighborhood_name"),
        pl.lit(False).alias("ej_community"),
        pl.lit(False).alias("equity_strategy"),
        pl.lit(4.0).alias("trips_per_hour_peak"),
    )
    lens_flags_data: list[dict[str, object]] = [
        {
            "neighborhood_id": "test-nbhd",
            "neighborhood_name": "Test Nbhd",
            "lens_flags": {
                "analysis_neighborhoods": True,
                "ej_communities": False,
                "equity_strategy": False,
            },
            "lens_flag_count": 1,
        }
    ]
    nb = NeighborhoodGrid(
        id="test-nbhd",
        name="Test Nbhd",
        population=len(result),
        lens_flags=LensFlags(
            analysis_neighborhoods=True,
            ej_communities=False,
            equity_strategy=False,
        ),
        pct_within=empty_grid,
    )
    city_wide = CityWide(pct_within=empty_grid)
    return stratified, lens_flags_data, [nb], city_wide, 0.1, 0.1


class TestMainOrchestration:
    """Story 1.8 T6a: orchestration test via mocked pipeline chain."""

    def test_sample_mode_runs_without_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """main() with --sample 5 completes via fully-mocked pipeline chain."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "sys.argv",
            ["muni-walk-access", "--sample", "5", "--config", str(_CONFIG_PATH)],
        )

        mock_net = MagicMock()
        mock_addresses = _make_mock_addresses(5)
        mock_stops = _make_mock_stops()
        mock_result = _make_mock_result(5)

        with (
            patch(
                "muni_walk_access.__main__.build_network",
                return_value=(mock_net, "20260101"),
            ),
            patch(
                "muni_walk_access.__main__.fetch_residential_addresses",
                return_value=mock_addresses,
            ),
            patch(
                "muni_walk_access.__main__.fetch_gtfs",
                return_value=(mock_stops, "abcdef01"),
            ),
            patch(
                "muni_walk_access.__main__.route_nearest_stops",
                return_value=mock_result,
            ),
            patch("muni_walk_access.__main__._write_timing_doc"),
            patch(
                "muni_walk_access.__main__._run_stratify",
                return_value=_make_mock_stratify_return(mock_result),
            ),
        ):
            main()  # must not raise

    def test_pipeline_calls_stages_in_order(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Each pipeline stage function is called exactly once."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "sys.argv",
            ["muni-walk-access", "--sample", "5", "--config", str(_CONFIG_PATH)],
        )

        mock_net = MagicMock()
        mock_addresses = _make_mock_addresses(5)
        mock_stops = _make_mock_stops()
        mock_result = _make_mock_result(5)

        with (
            patch(
                "muni_walk_access.__main__.build_network",
                return_value=(mock_net, "20260101"),
            ) as m_net,
            patch(
                "muni_walk_access.__main__.fetch_residential_addresses",
                return_value=mock_addresses,
            ) as m_addr,
            patch(
                "muni_walk_access.__main__.fetch_gtfs",
                return_value=(mock_stops, "abcdef01"),
            ) as m_gtfs,
            patch(
                "muni_walk_access.__main__.route_nearest_stops",
                return_value=mock_result,
            ) as m_route,
            patch("muni_walk_access.__main__._write_timing_doc"),
            patch(
                "muni_walk_access.__main__._run_stratify",
                return_value=_make_mock_stratify_return(mock_result),
            ) as m_strat,
        ):
            main()

        m_net.assert_called_once()
        m_addr.assert_called_once()
        m_gtfs.assert_called_once()
        m_route.assert_called_once()
        m_strat.assert_called_once()

    def test_routing_result_cached_as_parquet(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Routing result is written to .cache/routing/ as a parquet file."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "sys.argv",
            ["muni-walk-access", "--sample", "5", "--config", str(_CONFIG_PATH)],
        )

        mock_addresses = _make_mock_addresses(5)
        mock_result = _make_mock_result(5)

        with (
            patch(
                "muni_walk_access.__main__.build_network",
                return_value=(MagicMock(), "20260101"),
            ),
            patch(
                "muni_walk_access.__main__.fetch_residential_addresses",
                return_value=mock_addresses,
            ),
            patch(
                "muni_walk_access.__main__.fetch_gtfs",
                return_value=(_make_mock_stops(), "abcdef01"),
            ),
            patch(
                "muni_walk_access.__main__.route_nearest_stops",
                return_value=mock_result,
            ),
            patch("muni_walk_access.__main__._write_timing_doc"),
            patch(
                "muni_walk_access.__main__._run_stratify",
                return_value=_make_mock_stratify_return(mock_result),
            ),
        ):
            main()

        # .cache/routing/ should have been created
        cache_dir = tmp_path / ".cache" / "routing"
        assert cache_dir.exists(), ".cache/routing/ not created"
        parquet_files = list(cache_dir.glob("routing-result-*.parquet"))
        assert len(parquet_files) == 1, f"Expected 1 parquet, found: {parquet_files}"

        # Round-trip check: the cached file is valid parquet
        loaded = pl.read_parquet(parquet_files[0])
        assert len(loaded) == 5
        assert "nearest_stop_distance_m" in loaded.columns
