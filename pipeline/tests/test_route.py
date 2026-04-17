"""Tests for route.nearest_stop — Story 1.7."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandana
import pandas as pd
import polars as pl
import pytest

from muni_walk_access.config import Config, DevConfig, load_config
from muni_walk_access.route.nearest_stop import _METERS_PER_MILE, route_nearest_stops

# ---------------------------------------------------------------------------
# Test fixtures and helpers
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _full_config(**overrides: Any) -> Config:
    """Load real config.yaml and optionally patch top-level fields."""
    cfg = load_config(_CONFIG_PATH)
    if overrides:
        return cfg.model_copy(update=overrides)
    return cfg


def _make_tiny_network() -> pandana.Network:
    """Build a minimal 4-node pandana Network for testing.

    Topology (in lon/lat coords, roughly SF area):
        N0 (-122.435, 37.762) ---800m--- N1 (-122.427, 37.762)
        |                                        |
       400m                                    400m
        |                                        |
        N2 (-122.435, 37.758)           N3 (-122.427, 37.758)

    Edge weights are set explicitly to avoid geodesic rounding.
    We use simple values (edge_weight = 400 or 800) so walk_minutes is
    deterministic: (dist / 1609.34) * 20.
    """
    node_x = pd.Series([0.0, 800.0, 0.0, 800.0], index=[0, 1, 2, 3])
    node_y = pd.Series([0.0, 0.0, 400.0, 400.0], index=[0, 1, 2, 3])
    edge_from = pd.Series([0, 1, 0, 1])
    edge_to = pd.Series([1, 0, 2, 3])
    edge_weights = pd.DataFrame({"length": [800.0, 800.0, 400.0, 400.0]})
    return pandana.Network(
        node_x=node_x,
        node_y=node_y,
        edge_from=edge_from,
        edge_to=edge_to,
        edge_weights=edge_weights,
        twoway=True,
    )


def _make_addresses(
    rows: int = 3,
    as_string: bool = False,
) -> pl.DataFrame:
    """Minimal address DataFrame near node 0 (lon=0, lat=0) of the test network."""
    lats = [0.001 * i for i in range(rows)]
    lons = [0.001 * i for i in range(rows)]
    addresses_str = [f"{i} Test St" for i in range(rows)]
    if as_string:
        return pl.DataFrame(
            {
                "address": addresses_str,
                "latitude": [str(v) for v in lats],
                "longitude": [str(v) for v in lons],
            }
        )
    return pl.DataFrame(
        {
            "address": addresses_str,
            "latitude": lats,
            "longitude": lons,
        }
    )


def _make_stops(stop_id: str = "S_TEST") -> pl.DataFrame:
    """Minimal stops DataFrame: one stop at node 1 (lon=800, lat=0)."""
    return pl.DataFrame(
        {
            "stop_id": [stop_id],
            "stop_lat": [0.0],
            "stop_lon": [800.0],
            "trips_per_hour_peak": [4.0],
        }
    )


# ---------------------------------------------------------------------------
# T6a: Core functionality with a real tiny pandana Network
# ---------------------------------------------------------------------------


class TestRouteNearestStops:
    """Tests for route_nearest_stops() using a real pandana Network."""

    def test_output_has_expected_columns(self) -> None:
        """T6a: result has all required output columns."""
        net = _make_tiny_network()
        cfg = _full_config()
        result = route_nearest_stops(net, _make_addresses(2), _make_stops(), cfg)

        for col in (
            "nearest_stop_distance_m",
            "walk_minutes",
            "nearest_stop_id",
            "address",
            "latitude",
            "longitude",
        ):
            assert col in result.columns, f"Missing column: {col}"

    def test_walk_minutes_formula(self) -> None:
        """T6b: walk_minutes = (distance_m / 1609.34) * pace_min_per_mile."""
        net = _make_tiny_network()
        cfg = _full_config()
        result = route_nearest_stops(net, _make_addresses(1), _make_stops(), cfg)
        dist = result["nearest_stop_distance_m"][0]
        expected_minutes = (dist / _METERS_PER_MILE) * cfg.walking.pace_min_per_mile
        assert abs(result["walk_minutes"][0] - expected_minutes) < 1e-6

    def test_no_nan_distances(self) -> None:
        """T6f: reachable addresses have non-null distances.

        Fixture places addresses within search radius, so all should be
        reachable; post-Fix #4, unreachable rows are null-valued (tested
        separately in test_unreachable_rows_are_nulled).
        """
        net = _make_tiny_network()
        cfg = _full_config()
        result = route_nearest_stops(net, _make_addresses(3), _make_stops(), cfg)
        assert result["nearest_stop_distance_m"].null_count() == 0
        assert result["walk_minutes"].null_count() == 0
        assert result["nearest_stop_id"].null_count() == 0

    def test_nearest_stop_id_present(self) -> None:
        """T6a (cont.): nearest_stop_id matches the single stop in the fixture."""
        net = _make_tiny_network()
        cfg = _full_config()
        result = route_nearest_stops(
            net, _make_addresses(2), _make_stops("STOP42"), cfg
        )
        assert all(sid == "STOP42" for sid in result["nearest_stop_id"].to_list())

    # ---------------------------------------------------------------------------
    # T6c/T6d: Sample mode and full mode
    # ---------------------------------------------------------------------------

    def test_sample_mode_returns_n_rows(self) -> None:
        """T6c: sample mode with n=2 out of 5 addresses returns 2 rows."""
        net = _make_tiny_network()
        cfg = _full_config(dev=DevConfig(sample_size=2))
        result = route_nearest_stops(net, _make_addresses(5), _make_stops(), cfg)
        assert len(result) == 2

    def test_full_mode_returns_all_rows(self) -> None:
        """T6d: full mode (sample_size=None) returns all rows."""
        net = _make_tiny_network()
        cfg = _full_config(dev=DevConfig(sample_size=None))
        result = route_nearest_stops(net, _make_addresses(4), _make_stops(), cfg)
        assert len(result) == 4

    def test_seeded_sampling_is_reproducible(self) -> None:
        """T6e: two calls with same seed=42 produce identical output."""
        net1 = _make_tiny_network()
        net2 = _make_tiny_network()
        cfg = _full_config(dev=DevConfig(sample_size=3))
        addresses = _make_addresses(10)

        r1 = route_nearest_stops(net1, addresses, _make_stops(), cfg)
        r2 = route_nearest_stops(net2, addresses, _make_stops(), cfg)

        assert r1["address"].to_list() == r2["address"].to_list()

    # ---------------------------------------------------------------------------
    # T5: String-typed lat/lon from fetch_tabular
    # ---------------------------------------------------------------------------

    def test_string_lat_lon_cast_correctly(self) -> None:
        """T5a: Utf8 latitude/longitude columns are cast before routing."""
        net = _make_tiny_network()
        cfg = _full_config()
        addresses_str = _make_addresses(2, as_string=True)
        assert addresses_str["latitude"].dtype == pl.Utf8  # confirm fixture is Utf8

        result = route_nearest_stops(net, addresses_str, _make_stops(), cfg)
        assert "nearest_stop_distance_m" in result.columns
        assert result["nearest_stop_distance_m"].null_count() == 0

    # ---------------------------------------------------------------------------
    # T6g: WARNING logged when addresses hit maxdist clamp
    # ---------------------------------------------------------------------------

    def test_unreachable_rows_are_nulled(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Fix #4: unreachable addresses get null distance / walk_minutes / stop_id.

        Uses a two-node network with a 6000m edge so every address snaps to
        node 0 and the single stop is at node 1 — network distance 6000m >
        5000m maxdist → pandana reports no POI in range → output rows must
        be nulled (previously leaked garbage finite values). A WARNING about
        unreachable addresses must also be emitted.
        """
        # Two-node network: node 0 at origin, node 1 at lon=6000 (6000m edge)
        node_x = pd.Series([0.0, 6000.0], index=[0, 1])
        node_y = pd.Series([0.0, 0.0], index=[0, 1])
        edge_from = pd.Series([0, 1])
        edge_to = pd.Series([1, 0])
        edge_weights = pd.DataFrame({"length": [6000.0, 6000.0]})
        net_far = pandana.Network(
            node_x=node_x,
            node_y=node_y,
            edge_from=edge_from,
            edge_to=edge_to,
            edge_weights=edge_weights,
            twoway=True,
        )

        cfg = _full_config()

        stops_far = pl.DataFrame(
            {
                "stop_id": ["FAR_STOP"],
                "stop_lat": [0.0],
                "stop_lon": [6000.0],
                "trips_per_hour_peak": [1.0],
            }
        )

        addresses_near = pl.DataFrame(
            {
                "address": ["0 Near St", "1 Near St"],
                "latitude": [0.0, 0.001],
                "longitude": [0.0, 0.001],
            }
        )

        logger_name = "muni_walk_access.route.nearest_stop"
        with caplog.at_level(logging.WARNING, logger=logger_name):
            result = route_nearest_stops(net_far, addresses_near, stops_far, cfg)

        # All rows unreachable → all three columns null.
        assert result["nearest_stop_distance_m"].null_count() == len(result)
        assert result["walk_minutes"].null_count() == len(result)
        assert result["nearest_stop_id"].null_count() == len(result)

        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("unreachable" in msg for msg in warnings)

    def test_reachable_coexists_with_unreachable(self) -> None:
        """Mixed reachable / unreachable: reachable rows keep values, others null."""
        # Network: two components — one near origin (reachable to S1), one far.
        node_x = pd.Series([0.0, 100.0, 10000.0], index=[0, 1, 2])
        node_y = pd.Series([0.0, 0.0, 0.0], index=[0, 1, 2])
        edge_from = pd.Series([0, 1])
        edge_to = pd.Series([1, 0])
        edge_weights = pd.DataFrame({"length": [100.0, 100.0]})
        net = pandana.Network(
            node_x=node_x,
            node_y=node_y,
            edge_from=edge_from,
            edge_to=edge_to,
            edge_weights=edge_weights,
            twoway=True,
        )

        cfg = _full_config()
        stops = pl.DataFrame(
            {
                "stop_id": ["S1"],
                "stop_lat": [0.0],
                "stop_lon": [100.0],
                "trips_per_hour_peak": [4.0],
            }
        )
        # addr 0 near node 0 (reachable), addr 1 near node 2 (unreachable — isolated)
        addresses = pl.DataFrame(
            {
                "address": ["near", "far"],
                "latitude": [0.0, 0.0],
                "longitude": [0.0, 10000.0],
            }
        )
        result = route_nearest_stops(net, addresses, stops, cfg)
        # Exactly one reachable, one unreachable.
        assert result["nearest_stop_distance_m"].null_count() == 1
        assert result["nearest_stop_id"].null_count() == 1
