"""Tests for frequency classification — Story 1.4 (AC-6, T9h–T9k)."""

from __future__ import annotations

import polars as pl
import pytest

from muni_walk_access.config import Config, FrequencyBin, FrequencyConfig
from muni_walk_access.frequency.classify import classify_stops


def _config_with_bins(bins: list[FrequencyBin]) -> Config:
    """Build a Config with custom frequency bins, other fields from config.yaml."""
    from pathlib import Path

    from muni_walk_access.config import load_config

    base = load_config(Path(__file__).parent.parent / "config.yaml")
    new_freq = FrequencyConfig(
        bins=bins,
        peak_window=base.frequency.peak_window,
        peak_am_start=base.frequency.peak_am_start,
        peak_am_end=base.frequency.peak_am_end,
    )
    return base.model_copy(update={"frequency": new_freq})


DEFAULT_BINS = [
    FrequencyBin(id="high", max_headway_min=10),
    FrequencyBin(id="medium", max_headway_min=20),
    FrequencyBin(id="low", max_headway_min=None),
]


def _df(*trip_counts: float) -> pl.DataFrame:
    """Build a test DataFrame with given trips_per_hour_peak values."""
    return pl.DataFrame(
        {
            "stop_id": [f"S{i}" for i in range(len(trip_counts))],
            "trips_per_hour_peak": list(trip_counts),
        }
    )


class TestClassifyStops:
    """AC-6: classify_stops assigns correct frequency_bin values."""

    def _classify(self, *trip_counts: float) -> list[str]:
        config = _config_with_bins(DEFAULT_BINS)
        df = _df(*trip_counts)
        result = classify_stops(df, config)
        return result["frequency_bin"].to_list()

    def test_high_frequency_bin(self) -> None:
        """T9h: 8 trips/hr → headway 7.5 min → high."""
        bins = self._classify(8.0)
        assert bins == ["high"]

    def test_medium_frequency_bin(self) -> None:
        """T9h: 4 trips/hr → headway 15 min → medium."""
        bins = self._classify(4.0)
        assert bins == ["medium"]

    def test_low_frequency_bin(self) -> None:
        """T9h: 2 trips/hr → headway 30 min → low (catch-all)."""
        bins = self._classify(2.0)
        assert bins == ["low"]

    def test_zero_trips_classified_as_catchall(self) -> None:
        """T9i: 0 trips → headway ∞ → low (catch-all), not excluded."""
        bins = self._classify(0.0)
        assert bins == ["low"]

    def test_exact_boundary_high(self) -> None:
        """6 trips/hr → headway exactly 10 min → high (≤ 10)."""
        bins = self._classify(6.0)
        assert bins == ["high"]

    def test_exact_boundary_medium(self) -> None:
        """3 trips/hr → headway exactly 20 min → medium (≤ 20)."""
        bins = self._classify(3.0)
        assert bins == ["medium"]

    def test_multiple_stops_mixed(self) -> None:
        """T9h: mixed trip counts produce correct bins."""
        bins = self._classify(8.0, 4.0, 2.0, 0.0)
        assert bins == ["high", "medium", "low", "low"]

    def test_original_columns_preserved(self) -> None:
        """Result DataFrame includes original columns plus frequency_bin."""
        config = _config_with_bins(DEFAULT_BINS)
        df = _df(5.0)
        result = classify_stops(df, config)
        assert "stop_id" in result.columns
        assert "trips_per_hour_peak" in result.columns
        assert "frequency_bin" in result.columns

    def test_no_extra_columns(self) -> None:
        """No _headway_min or other intermediate column leaks into result."""
        config = _config_with_bins(DEFAULT_BINS)
        result = classify_stops(_df(4.0), config)
        assert "_headway_min" not in result.columns


class TestBinValidation:
    """T9k: classify_stops raises ValueError for misconfigured bins."""

    def test_no_catchall_raises(self) -> None:
        """Missing catch-all bin (null max_headway_min) → ValueError."""
        bins = [
            FrequencyBin(id="high", max_headway_min=10),
            FrequencyBin(id="medium", max_headway_min=20),
        ]
        config = _config_with_bins(bins)
        with pytest.raises(ValueError, match="catch-all"):
            classify_stops(_df(4.0), config)

    def test_multiple_catchall_raises(self) -> None:
        """Two catch-all bins → ValueError."""
        bins = [
            FrequencyBin(id="high", max_headway_min=10),
            FrequencyBin(id="low1", max_headway_min=None),
            FrequencyBin(id="low2", max_headway_min=None),
        ]
        config = _config_with_bins(bins)
        with pytest.raises(ValueError, match="multiple"):
            classify_stops(_df(4.0), config)

    def test_catchall_not_last_raises(self) -> None:
        """Catch-all not in last position → ValueError."""
        bins = [
            FrequencyBin(id="low", max_headway_min=None),
            FrequencyBin(id="high", max_headway_min=10),
        ]
        config = _config_with_bins(bins)
        with pytest.raises(ValueError, match="last"):
            classify_stops(_df(4.0), config)

    def test_unordered_bins_raises(self) -> None:
        """Non-ascending max_headway_min for bounded bins → ValueError."""
        bins = [
            FrequencyBin(id="medium", max_headway_min=20),
            FrequencyBin(id="high", max_headway_min=10),
            FrequencyBin(id="low", max_headway_min=None),
        ]
        config = _config_with_bins(bins)
        with pytest.raises(ValueError, match="ascending"):
            classify_stops(_df(4.0), config)
