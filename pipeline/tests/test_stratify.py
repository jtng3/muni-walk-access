"""Tests for stratify.lens and stratify.grid — Story 1.9."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import box

from muni_walk_access.config import Config, load_config
from muni_walk_access.stratify.grid import compute_grid, compute_hex_grids
from muni_walk_access.stratify.lens import (
    aggregate_to_lenses,
    compute_lens_flags,
    slugify_neighborhood,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _full_config(**overrides: Any) -> Config:
    """Load real config.yaml and optionally patch top-level fields."""
    cfg = load_config(_CONFIG_PATH)
    if overrides:
        return cfg.model_copy(update=overrides)
    return cfg


def _make_routing_result(
    lats: list[float],
    lons: list[float],
    stop_ids: list[str | None] | None = None,
) -> pl.DataFrame:
    """Minimal routing-result DataFrame matching Story 1.8 schema."""
    n = len(lats)
    return pl.DataFrame(
        {
            "address": [f"addr-{i}" for i in range(n)],
            "latitude": lats,
            "longitude": lons,
            "nearest_stop_distance_m": [100.0] * n,
            "walk_minutes": [2.0] * n,
            "nearest_stop_id": stop_ids or ["S0"] * n,
        }
    )


def _make_stops(
    stop_ids: list[str] | None = None,
    trips: list[float] | None = None,
) -> pl.DataFrame:
    """Minimal stops DataFrame."""
    ids = stop_ids or ["S0", "S1"]
    tph = trips or [6.0, 3.0]
    return pl.DataFrame(
        {
            "stop_id": ids,
            "stop_lat": [37.78] * len(ids),
            "stop_lon": [-122.41] * len(ids),
            "trips_per_hour_peak": tph,
        }
    )


def _make_mock_boundaries() -> dict[str, gpd.GeoDataFrame]:
    """Create synthetic boundary GeoDataFrames for testing.

    Layout (lon, lat coordinates):
        Neighbourhood A:  box(-122.5, 37.7, -122.4, 37.8)
        Neighbourhood B:  box(-122.4, 37.7, -122.3, 37.8)
        EJ community:     covers only A's box
        Equity strategy:  covers both A and B boxes
    """
    poly_a = box(-122.5, 37.7, -122.4, 37.8)
    poly_b = box(-122.4, 37.7, -122.3, 37.8)
    poly_ej = box(-122.5, 37.7, -122.4, 37.8)  # same as A
    poly_eq = box(-122.5, 37.7, -122.3, 37.8)  # spans both

    ana = gpd.GeoDataFrame(
        {"nhood": ["Neighbourhood A", "Neighbourhood B"]},
        geometry=[poly_a, poly_b],
        crs="EPSG:4326",
    )
    ej = gpd.GeoDataFrame(
        {"id": ["ej1"]},
        geometry=[poly_ej],
        crs="EPSG:4326",
    )
    eq = gpd.GeoDataFrame(
        {"id": ["eq1"]},
        geometry=[poly_eq],
        crs="EPSG:4326",
    )
    return {
        "analysis_neighborhoods": ana,
        "ej_communities": ej,
        "equity_strategy": eq,
    }


# ---------------------------------------------------------------------------
# T2: slugify_neighborhood tests
# ---------------------------------------------------------------------------


class TestSlugifyNeighborhood:
    """T2b: slugify_neighborhood edge cases."""

    def test_basic_space(self) -> None:
        """Space-separated name becomes kebab-case."""
        assert slugify_neighborhood("Outer Mission") == "outer-mission"

    def test_no_change(self) -> None:
        """Already-lowercase single word is unchanged."""
        assert slugify_neighborhood("tenderloin") == "tenderloin"

    def test_apostrophe(self) -> None:
        """Apostrophe is stripped, not replaced with hyphen."""
        assert slugify_neighborhood("Fisherman's Wharf") == "fishermans-wharf"

    def test_slash(self) -> None:
        """Slash is replaced with hyphen."""
        result = slugify_neighborhood("South of Market/SoMa")
        assert result == "south-of-market-soma"

    def test_simple_name(self) -> None:
        """Two-word name becomes kebab-case."""
        assert slugify_neighborhood("Bernal Heights") == "bernal-heights"

    def test_leading_trailing_special(self) -> None:
        """Leading/trailing hyphens are stripped."""
        assert slugify_neighborhood("--Mission Bay--") == "mission-bay"

    def test_consecutive_hyphens(self) -> None:
        """Consecutive special chars collapse to single hyphen."""
        assert slugify_neighborhood("A   -  B") == "a-b"

    def test_parentheses(self) -> None:
        """Parentheses are replaced with hyphens."""
        result = slugify_neighborhood("Bayview (Hunters Point)")
        assert result == "bayview-hunters-point"


# ---------------------------------------------------------------------------
# T3: aggregate_to_lenses tests
# ---------------------------------------------------------------------------


class TestAggregateToLenses:
    """T3c: Spatial join with synthetic geometries."""

    def test_addresses_assigned_correct_neighbourhood(self) -> None:
        """Addresses inside known polygons get correct assignment."""
        config = _full_config()
        # Two points: one in A, one in B
        routing = _make_routing_result(
            lats=[37.75, 37.75],
            lons=[-122.45, -122.35],
        )
        stops = _make_stops()
        boundaries = _make_mock_boundaries()

        with patch(
            "muni_walk_access.stratify.lens._fetch_boundaries",
            return_value=boundaries,
        ):
            result = aggregate_to_lenses(routing, stops, config)

        assert "neighborhood_id" in result.columns
        assert "neighborhood_name" in result.columns
        ids = result["neighborhood_id"].sort().to_list()
        assert ids == ["neighbourhood-a", "neighbourhood-b"]

    def test_outside_addresses_excluded(self) -> None:
        """Addresses outside all boundaries are excluded."""
        config = _full_config()
        routing = _make_routing_result(
            lats=[37.75, 37.0],  # second is way outside
            lons=[-122.45, -121.0],
        )
        stops = _make_stops()
        boundaries = _make_mock_boundaries()

        with patch(
            "muni_walk_access.stratify.lens._fetch_boundaries",
            return_value=boundaries,
        ):
            result = aggregate_to_lenses(routing, stops, config)

        assert len(result) == 1
        assert result["neighborhood_name"][0] == "Neighbourhood A"

    def test_ej_and_equity_flags(self) -> None:
        """EJ community and equity strategy booleans correctly assigned."""
        config = _full_config()
        routing = _make_routing_result(
            lats=[37.75, 37.75],
            lons=[-122.45, -122.35],
        )
        stops = _make_stops()
        boundaries = _make_mock_boundaries()

        with patch(
            "muni_walk_access.stratify.lens._fetch_boundaries",
            return_value=boundaries,
        ):
            result = aggregate_to_lenses(routing, stops, config)

        # Sort by neighbourhood for deterministic order
        result = result.sort("neighborhood_id")
        # A is in EJ, both are in equity strategy
        assert result["ej_community"][0] is True  # A
        assert result["ej_community"][1] is False  # B
        assert result["equity_strategy"][0] is True  # A
        assert result["equity_strategy"][1] is True  # B

    def test_trips_per_hour_peak_joined(self) -> None:
        """trips_per_hour_peak from stops_df is joined via nearest_stop_id."""
        config = _full_config()
        routing = _make_routing_result(
            lats=[37.75],
            lons=[-122.45],
            stop_ids=["S1"],
        )
        stops = _make_stops(stop_ids=["S0", "S1"], trips=[6.0, 3.0])
        boundaries = _make_mock_boundaries()

        with patch(
            "muni_walk_access.stratify.lens._fetch_boundaries",
            return_value=boundaries,
        ):
            result = aggregate_to_lenses(routing, stops, config)

        assert result["trips_per_hour_peak"][0] == pytest.approx(3.0)

    def test_null_stop_id_gets_null_trips(self) -> None:
        """Addresses with null nearest_stop_id get null trips_per_hour_peak."""
        config = _full_config()
        routing = _make_routing_result(
            lats=[37.75],
            lons=[-122.45],
            stop_ids=[None],
        )
        stops = _make_stops()
        boundaries = _make_mock_boundaries()

        with patch(
            "muni_walk_access.stratify.lens._fetch_boundaries",
            return_value=boundaries,
        ):
            result = aggregate_to_lenses(routing, stops, config)

        assert result["trips_per_hour_peak"].null_count() == 1

    def test_empty_input_returns_empty_with_columns(self) -> None:
        """Empty routing result returns empty DataFrame with expected cols."""
        config = _full_config()
        routing = _make_routing_result(lats=[], lons=[])
        stops = _make_stops()

        result = aggregate_to_lenses(routing, stops, config)

        assert len(result) == 0
        for col in [
            "neighborhood_id",
            "neighborhood_name",
            "ej_community",
            "equity_strategy",
            "trips_per_hour_peak",
        ]:
            assert col in result.columns


# ---------------------------------------------------------------------------
# T4: compute_lens_flags tests
# ---------------------------------------------------------------------------


class TestComputeLensFlags:
    """T4b: Lens flag computation for mixed-membership neighbourhoods."""

    def test_mixed_membership_flags(self) -> None:
        """Neighbourhood with mixed EJ/equity membership gets correct flags."""
        config = _full_config()
        stratified = pl.DataFrame(
            {
                "neighborhood_id": ["a", "a", "b", "b"],
                "neighborhood_name": ["A", "A", "B", "B"],
                "ej_community": [True, False, False, False],
                "equity_strategy": [True, True, False, False],
            }
        )
        flags = compute_lens_flags(stratified, config)

        assert len(flags) == 2
        # Sorted by ID: a first, b second
        a_flags = flags[0]
        assert a_flags["neighborhood_id"] == "a"
        assert a_flags["lens_flags"]["analysis_neighborhoods"] is True
        assert a_flags["lens_flags"]["ej_communities"] is True
        assert a_flags["lens_flags"]["equity_strategy"] is True
        assert a_flags["lens_flag_count"] == 3

        b_flags = flags[1]
        assert b_flags["lens_flags"]["ej_communities"] is False
        assert b_flags["lens_flags"]["equity_strategy"] is False
        assert b_flags["lens_flag_count"] == 1  # only analysis_neighborhoods

    def test_empty_input(self) -> None:
        """Empty stratified DataFrame returns empty list."""
        config = _full_config()
        stratified = pl.DataFrame(
            {
                "neighborhood_id": [],
                "neighborhood_name": [],
                "ej_community": [],
                "equity_strategy": [],
            },
            schema={
                "neighborhood_id": pl.Utf8,
                "neighborhood_name": pl.Utf8,
                "ej_community": pl.Boolean,
                "equity_strategy": pl.Boolean,
            },
        )
        assert compute_lens_flags(stratified, config) == []


# ---------------------------------------------------------------------------
# T5: compute_grid tests
# ---------------------------------------------------------------------------


def _make_stratified(
    n: int = 10,
    nbhd_id: str = "test-nbhd",
    nbhd_name: str = "Test Nbhd",
    walk_minutes: float = 4.0,
    trips_per_hour_peak: float = 8.0,
) -> pl.DataFrame:
    """Build a stratified DataFrame with uniform per-address values."""
    return pl.DataFrame(
        {
            "address": [f"addr-{i}" for i in range(n)],
            "latitude": [37.78] * n,
            "longitude": [-122.41] * n,
            "nearest_stop_distance_m": [100.0] * n,
            "walk_minutes": [walk_minutes] * n,
            "nearest_stop_id": ["S0"] * n,
            "neighborhood_id": [nbhd_id] * n,
            "neighborhood_name": [nbhd_name] * n,
            "ej_community": [False] * n,
            "equity_strategy": [True] * n,
            "trips_per_hour_peak": [trips_per_hour_peak] * n,
        }
    )


class TestComputeGrid:
    """T5g: Grid dimensions, cell ranges, precision, sort order."""

    def test_grid_dimensions(self) -> None:
        """Grid has len(freq) rows and len(walk) cols."""
        config = _full_config()
        strat = _make_stratified()
        neighborhoods, city_wide = compute_grid(strat, config)

        n_freq = len(config.grid.frequency_threshold_min)
        n_walk = len(config.grid.walking_minutes)

        assert len(neighborhoods) == 1
        nb = neighborhoods[0]
        assert len(nb.pct_within) == n_freq
        for row in nb.pct_within:
            assert len(row) == n_walk

        assert len(city_wide.pct_within) == n_freq
        for row in city_wide.pct_within:
            assert len(row) == n_walk

    def test_all_cells_in_unit_range(self) -> None:
        """Every pct_within cell is in [0.0, 1.0]."""
        config = _full_config()
        strat = _make_stratified()
        neighborhoods, city_wide = compute_grid(strat, config)

        for nb in neighborhoods:
            for row in nb.pct_within:
                for val in row:
                    assert 0.0 <= val <= 1.0

        for row in city_wide.pct_within:
            for val in row:
                assert 0.0 <= val <= 1.0

    def test_four_decimal_precision(self) -> None:
        """Grid cells are rounded to 4 decimal places."""
        config = _full_config()
        # 3 out of 7 addresses meet a threshold → 0.4286 (4 decimals)
        strat = pl.DataFrame(
            {
                "address": [f"a{i}" for i in range(7)],
                "latitude": [37.78] * 7,
                "longitude": [-122.41] * 7,
                "nearest_stop_distance_m": [100.0] * 7,
                "walk_minutes": [2.0] * 3 + [20.0] * 4,
                "nearest_stop_id": ["S0"] * 7,
                "neighborhood_id": ["n1"] * 7,
                "neighborhood_name": ["N1"] * 7,
                "ej_community": [False] * 7,
                "equity_strategy": [False] * 7,
                "trips_per_hour_peak": [10.0] * 7,
            }
        )
        neighborhoods, _ = compute_grid(strat, config)
        nb = neighborhoods[0]
        # walk_minutes=3 threshold: 3 out of 7 addresses have walk<=3
        # (walk_minutes=2.0 for first 3). 3/7 = 0.4286 (rounded to 4dp)
        # freq_threshold=4 → trips_needed=15.0, trips=10.0 → fails
        # freq_threshold=6 → trips_needed=10.0, trips=10.0 → passes
        # So at freq_idx=1 (6-min), walk_idx=0 (3-min): 3/7 = 0.4286
        assert nb.pct_within[1][0] == 0.4286

    def test_sorted_by_id(self) -> None:
        """Neighbourhoods are sorted ascending by id."""
        config = _full_config()
        strat_z = _make_stratified(n=5, nbhd_id="z-nbhd", nbhd_name="Z")
        strat_a = _make_stratified(n=5, nbhd_id="a-nbhd", nbhd_name="A")
        strat = pl.concat([strat_z, strat_a])

        neighborhoods, _ = compute_grid(strat, config)
        ids = [nb.id for nb in neighborhoods]
        assert ids == sorted(ids)

    def test_city_wide_population_weighted(self) -> None:
        """City-wide is the population-weighted average of neighbourhoods."""
        config = _full_config()
        # A: 8 addresses, all meet every threshold
        strat_a = _make_stratified(
            n=8,
            nbhd_id="a",
            nbhd_name="A",
            walk_minutes=1.0,
            trips_per_hour_peak=20.0,
        )
        # B: 2 addresses, none meet any threshold
        strat_b = _make_stratified(
            n=2,
            nbhd_id="b",
            nbhd_name="B",
            walk_minutes=1.0,
            trips_per_hour_peak=0.5,
        )
        strat = pl.concat([strat_a, strat_b])
        _, city_wide = compute_grid(strat, config)

        # For any cell: A pct=1.0 (all 8 meet), B pct=0.0 (none meet)
        # City-wide = (1.0*8 + 0.0*2) / 10 = 0.8
        assert city_wide.pct_within[0][0] == 0.8

    def test_empty_input(self) -> None:
        """Empty stratified DataFrame returns empty neighborhoods + zero grid."""
        config = _full_config()
        strat = _make_stratified(n=0)
        neighborhoods, city_wide = compute_grid(strat, config)

        assert neighborhoods == []
        for row in city_wide.pct_within:
            for val in row:
                assert val == 0.0

    def test_null_trips_never_meet_threshold(self) -> None:
        """Addresses with null trips_per_hour_peak don't meet frequency criteria."""
        config = _full_config()
        strat = pl.DataFrame(
            {
                "address": ["a0", "a1"],
                "latitude": [37.78, 37.78],
                "longitude": [-122.41, -122.41],
                "nearest_stop_distance_m": [100.0, 100.0],
                "walk_minutes": [1.0, 1.0],
                "nearest_stop_id": ["S0", None],
                "neighborhood_id": ["n1", "n1"],
                "neighborhood_name": ["N1", "N1"],
                "ej_community": [False, False],
                "equity_strategy": [False, False],
                "trips_per_hour_peak": [20.0, None],
            }
        )
        neighborhoods, _ = compute_grid(strat, config)
        nb = neighborhoods[0]
        # 1 out of 2 addresses meets all thresholds → 0.5
        assert nb.pct_within[0][0] == 0.5


# ---------------------------------------------------------------------------
# T8: Determinism tests
# ---------------------------------------------------------------------------


class TestDeterminism:
    """T8a-b: Deterministic output and sort order."""

    def test_identical_runs_produce_identical_output(self) -> None:
        """compute_grid() called twice → byte-identical serialised JSON."""
        config = _full_config()
        strat = pl.concat(
            [
                _make_stratified(n=10, nbhd_id="a", nbhd_name="A"),
                _make_stratified(n=5, nbhd_id="b", nbhd_name="B"),
            ]
        )

        nbs1, cw1 = compute_grid(strat, config)
        nbs2, cw2 = compute_grid(strat, config)

        json1 = json.dumps(
            {
                "neighborhoods": [nb.model_dump() for nb in nbs1],
                "city_wide": cw1.model_dump(),
            },
            sort_keys=True,
        )
        json2 = json.dumps(
            {
                "neighborhoods": [nb.model_dump() for nb in nbs2],
                "city_wide": cw2.model_dump(),
            },
            sort_keys=True,
        )
        assert json1 == json2

    def test_neighbourhoods_sorted_ascending_by_id(self) -> None:
        """Neighbourhoods list is sorted ascending by id."""
        config = _full_config()
        strat = pl.concat(
            [
                _make_stratified(n=3, nbhd_id="z-last", nbhd_name="Z"),
                _make_stratified(n=3, nbhd_id="a-first", nbhd_name="A"),
                _make_stratified(n=3, nbhd_id="m-mid", nbhd_name="M"),
            ]
        )
        neighborhoods, _ = compute_grid(strat, config)
        ids = [nb.id for nb in neighborhoods]
        assert ids == ["a-first", "m-mid", "z-last"]


# ---------------------------------------------------------------------------
# compute_hex_grid tests (Story 1.11)
# ---------------------------------------------------------------------------


class TestComputeHexGrids:
    """Tests for compute_hex_grids — multi-resolution H3 cell assignment."""

    def test_returns_dict_of_resolutions(self) -> None:
        """compute_hex_grids returns a dict keyed by resolution."""
        config = _full_config()
        strat = _make_stratified_multi(
            [37.75, 37.78, 37.76], [-122.45, -122.41, -122.43]
        )
        result = compute_hex_grids(strat, config, resolutions=[7, 8])
        assert set(result.keys()) == {7, 8}

    def test_default_resolutions_7_through_11(self) -> None:
        """Default resolutions are 7-11 inclusive."""
        config = _full_config()
        strat = _make_stratified_multi([37.75], [-122.45])
        result = compute_hex_grids(strat, config)
        assert set(result.keys()) == set(range(7, 12))

    def test_cell_ids_valid_h3_at_correct_resolution(self) -> None:
        """All returned cell IDs are valid H3 cells at their stated resolution."""
        import h3

        config = _full_config()
        strat = _make_stratified_multi([37.75, 37.76], [-122.45, -122.44])
        result = compute_hex_grids(strat, config, resolutions=[7, 8, 9])
        for res, cells in result.items():
            for cell in cells:
                assert h3.is_valid_cell(cell.id)
                assert h3.get_resolution(cell.id) == res

    def test_pct_within_shape_matches_axes(self) -> None:
        """Every cell's pct_within has shape [n_freq][n_walk]."""
        config = _full_config()
        strat = _make_stratified_multi([37.75], [-122.45])
        result = compute_hex_grids(strat, config, resolutions=[8])
        n_freq = len(config.grid.frequency_threshold_min)
        n_walk = len(config.grid.walking_minutes)
        for cell in result[8]:
            assert len(cell.pct_within) == n_freq
            for row in cell.pct_within:
                assert len(row) == n_walk

    def test_pct_within_values_in_unit_range(self) -> None:
        """All pct_within values are in [0.0, 1.0]."""
        config = _full_config()
        strat = _make_stratified_multi(
            [37.75, 37.76, 37.77], [-122.45, -122.44, -122.43]
        )
        result = compute_hex_grids(strat, config, resolutions=[8])
        for cell in result[8]:
            for row in cell.pct_within:
                for val in row:
                    assert 0.0 <= val <= 1.0

    def test_higher_res_more_cells(self) -> None:
        """Higher resolution produces more cells from the same address set."""
        config = _full_config()
        lats = [37.72, 37.75, 37.78, 37.80]
        lons = [-122.40, -122.45, -122.42, -122.46]
        strat = _make_stratified_multi(lats, lons)
        result = compute_hex_grids(strat, config, resolutions=[6, 8, 10])
        # More addresses in same area → more unique cells at higher res
        assert len(result[10]) >= len(result[8]) >= len(result[6])

    def test_population_matches_addresses_in_cell(self) -> None:
        """Cell population equals number of addresses assigned to that cell."""
        import h3

        config = _full_config()
        lat, lon = 37.7612, -122.4187
        strat = _make_stratified_multi([lat, lat + 0.0001], [lon, lon + 0.0001])
        result = compute_hex_grids(strat, config, resolutions=[8])
        cell_id = h3.latlng_to_cell(lat, lon, 8)
        matching = [c for c in result[8] if c.id == cell_id]
        assert len(matching) == 1
        assert matching[0].population == 2

    def test_center_coords_in_sf_range(self) -> None:
        """center_lat and center_lon are in SF geographic range."""
        config = _full_config()
        strat = _make_stratified_multi([37.75], [-122.45])
        result = compute_hex_grids(strat, config, resolutions=[8])
        for cell in result[8]:
            assert 37.0 <= cell.center_lat <= 38.5
            assert -123.5 <= cell.center_lon <= -121.5

    def test_empty_input_returns_empty_lists(self) -> None:
        """Empty stratified DataFrame returns empty lists for all resolutions."""
        config = _full_config()
        strat = _make_stratified(n=0)
        result = compute_hex_grids(strat, config, resolutions=[7, 8])
        assert result[7] == []
        assert result[8] == []

    def test_cells_sorted_by_id_per_resolution(self) -> None:
        """Returned cells are sorted ascending by H3 cell ID."""
        config = _full_config()
        lats = [37.72, 37.75, 37.78, 37.80]
        lons = [-122.40, -122.45, -122.42, -122.46]
        strat = _make_stratified_multi(lats, lons)
        result = compute_hex_grids(strat, config, resolutions=[8])
        ids = [c.id for c in result[8]]
        assert ids == sorted(ids)

    def test_uses_latitude_longitude_columns(self) -> None:
        """compute_hex_grids reads 'latitude'/'longitude' (not 'lat'/'lon')."""
        import h3

        config = _full_config()
        lat, lon = 37.7612, -122.4187
        strat = _make_stratified_multi([lat], [lon])
        assert "latitude" in strat.columns
        assert "longitude" in strat.columns
        result = compute_hex_grids(strat, config, resolutions=[8])
        expected_cell_id = h3.latlng_to_cell(lat, lon, 8)
        assert any(c.id == expected_cell_id for c in result[8])


def _make_stratified_multi(
    lats: list[float],
    lons: list[float],
    walk_minutes: float = 4.0,
    trips_per_hour_peak: float = 8.0,
) -> pl.DataFrame:
    """Build a stratified DataFrame with multiple lat/lon points."""
    n = len(lats)
    return pl.DataFrame(
        {
            "address": [f"addr-{i}" for i in range(n)],
            "latitude": lats,
            "longitude": lons,
            "nearest_stop_distance_m": [100.0] * n,
            "walk_minutes": [walk_minutes] * n,
            "nearest_stop_id": ["S0"] * n,
            "neighborhood_id": ["test-nbhd"] * n,
            "neighborhood_name": ["Test Nbhd"] * n,
            "ej_community": [False] * n,
            "equity_strategy": [True] * n,
            "trips_per_hour_peak": [trips_per_hour_peak] * n,
        }
    )


# ---------------------------------------------------------------------------
# T5 filter engine + BoundarySource tests (Story 5.3 / AC-3, AC-11)
# ---------------------------------------------------------------------------


class TestApplyLensFilter:
    """AC-3: generic lens-boundary filter engine replaces the SF `lens.id` branch."""

    @staticmethod
    def _lens(**overrides: Any):
        from muni_walk_access.config import LensConfig

        base = {"id": "test_lens", "datasf_id": "abcd-1234", "label": "Test Lens"}
        base.update(overrides)
        return LensConfig(**base)

    @staticmethod
    def _gdf(**cols: Any) -> gpd.GeoDataFrame:
        """Build a small polygon-free gdf (filter engine only touches attributes)."""
        from shapely.geometry import Point

        n = len(next(iter(cols.values())))
        return gpd.GeoDataFrame(
            cols | {"geometry": [Point(0, i) for i in range(n)]},
            crs="EPSG:4326",
        )

    def test_no_filter_is_passthrough(self) -> None:
        """Unconfigured lens returns gdf unchanged."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens()
        gdf = self._gdf(value=[1, 2, 3])
        assert len(_apply_lens_filter(gdf, lens)) == 3

    def test_score_threshold_keeps_ge_threshold(self) -> None:
        """SF EJ regression: score >= 21 retained, lower scores dropped."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens(score_field="score", score_threshold=21)
        gdf = self._gdf(score=["10", "20", "21", "25", "30"])
        out = _apply_lens_filter(gdf, lens)
        assert sorted(out["score"].tolist()) == [21.0, 25.0, 30.0]

    def test_score_threshold_null_rows_excluded(self) -> None:
        """Non-numeric score coerces to NaN, which fails the threshold → dropped."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens(score_field="score", score_threshold=21)
        gdf = self._gdf(score=["25", "bogus", "30"])
        out = _apply_lens_filter(gdf, lens)
        assert sorted(out["score"].tolist()) == [25.0, 30.0]

    def test_score_threshold_missing_column_warns_and_passes(self) -> None:
        """Missing score column → warn + pass-through (matches old EJ fallback)."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens(score_field="score", score_threshold=21)
        gdf = self._gdf(other_col=[1, 2, 3])
        assert len(_apply_lens_filter(gdf, lens)) == 3

    def test_filter_op_eq(self) -> None:
        """filter_op='eq' keeps rows where field equals value."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens(filter_field="zone", filter_op="eq", filter_value="A")
        gdf = self._gdf(zone=["A", "B", "A", "C"])
        out = _apply_lens_filter(gdf, lens)
        assert out["zone"].tolist() == ["A", "A"]

    def test_filter_op_ne(self) -> None:
        """filter_op='ne' keeps rows where field does not equal value."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens(filter_field="zone", filter_op="ne", filter_value="A")
        gdf = self._gdf(zone=["A", "B", "A", "C"])
        out = _apply_lens_filter(gdf, lens)
        assert out["zone"].tolist() == ["B", "C"]

    def test_filter_op_gte_and_lte(self) -> None:
        """filter_op='gte' and 'lte' produce expected half-open subsets."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens_gte = self._lens(filter_field="rank", filter_op="gte", filter_value=3)
        lens_lte = self._lens(filter_field="rank", filter_op="lte", filter_value=3)
        gdf = self._gdf(rank=[1, 2, 3, 4, 5])
        assert sorted(_apply_lens_filter(gdf, lens_gte)["rank"].tolist()) == [3, 4, 5]
        assert sorted(_apply_lens_filter(gdf, lens_lte)["rank"].tolist()) == [1, 2, 3]

    def test_filter_op_in(self) -> None:
        """filter_op='in' keeps rows whose field matches any list member."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens(filter_field="zone", filter_op="in", filter_value=["A", "C"])
        gdf = self._gdf(zone=["A", "B", "C", "D"])
        out = _apply_lens_filter(gdf, lens)
        assert sorted(out["zone"].tolist()) == ["A", "C"]

    def test_filter_op_in_requires_list(self) -> None:
        """filter_op='in' with scalar filter_value raises with a clear message."""
        from muni_walk_access.config import LensConfig
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        # LensConfig validator allows scalar filter_value — guard raises at apply time.
        lens = LensConfig.model_construct(
            id="test_lens",
            datasf_id="x",
            label="X",
            source_kind="datasf",
            name_field="nhood",
            filter_field="zone",
            filter_op="in",
            filter_value="A",  # scalar instead of list
            score_field=None,
            score_threshold=None,
        )
        gdf = self._gdf(zone=["A", "B"])
        with pytest.raises(ValueError, match="requires filter_value to be a list"):
            _apply_lens_filter(gdf, lens)

    def test_score_and_filter_compose(self) -> None:
        """Score filter runs first, then attribute filter."""
        from muni_walk_access.ingest.boundaries import _apply_lens_filter

        lens = self._lens(
            score_field="score",
            score_threshold=10,
            filter_field="zone",
            filter_op="eq",
            filter_value="A",
        )
        gdf = self._gdf(
            score=["5", "15", "20", "25"],
            zone=["A", "A", "B", "A"],
        )
        out = _apply_lens_filter(gdf, lens)
        # score >= 10 leaves rows 1,2,3 (score 15,20,25); then zone==A leaves 1,3.
        assert sorted(out["score"].tolist()) == [15.0, 25.0]


class TestBoundarySourceDispatch:
    """AC-1: BOUNDARY_SOURCES registry + NotImplementedError stubs."""

    def test_datasf_registered(self) -> None:
        """DataSFBoundarySource is registered under the 'datasf' source_kind."""
        # Triggers registration of DataSFBoundarySource at module import.
        import muni_walk_access.ingest.sources.datasf  # noqa: F401
        from muni_walk_access.ingest.boundaries import get_boundary_source

        cls = get_boundary_source("datasf")
        assert cls.__name__ == "DataSFBoundarySource"

    def test_arcgis_hub_stub_raises(self) -> None:
        """ArcGISHubBoundarySource raises NotImplementedError pointing at 5-4."""
        from muni_walk_access.config import LensConfig
        from muni_walk_access.ingest.boundaries import get_boundary_source

        lens = LensConfig(id="pd", datasf_id="x", label="L", source_kind="arcgis_hub")
        src = get_boundary_source("arcgis_hub")()
        with pytest.raises(NotImplementedError, match="Story 5-4"):
            src.fetch(lens)

    def test_generic_url_stub_raises(self) -> None:
        """GenericURLBoundarySource raises NotImplementedError pointing at 5-4."""
        from muni_walk_access.config import LensConfig
        from muni_walk_access.ingest.boundaries import get_boundary_source

        lens = LensConfig(id="ejx", datasf_id="x", label="L", source_kind="generic_url")
        src = get_boundary_source("generic_url")()
        with pytest.raises(NotImplementedError, match="Story 5-4"):
            src.fetch(lens)

    def test_unknown_kind_raises_keyerror(self) -> None:
        """Unregistered source_kind raises KeyError naming the missing kind."""
        from muni_walk_access.ingest.boundaries import get_boundary_source

        with pytest.raises(KeyError, match="No BoundarySource"):
            get_boundary_source("does_not_exist")

    def test_datasf_boundary_source_handles_crs_less_gdf(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: CRS-less GeoJSON uses set_crs, not to_crs (5-4 safety net).

        DataSF's SODA endpoint always ships WGS84 metadata so SF never exercises
        this branch; Philly's PennEnviroScreen (Story 5-4 via
        `GenericURLBoundarySource`) may ship CRS-less geometry. Testing here
        pins the safe `set_crs` path and prevents a pyproj raise when the
        shared pattern lands for the generic adapter.
        """
        from shapely.geometry import box

        from muni_walk_access.config import LensConfig, load_config
        from muni_walk_access.ingest.cache import CacheManager
        from muni_walk_access.ingest.sources.datasf import DataSFBoundarySource
        from muni_walk_access.run_context import RunContext

        # Write a CRS-less geojson the adapter will read via fetch_geospatial.
        crs_less_gdf = gpd.GeoDataFrame(
            {"nhood": ["X"]}, geometry=[box(-122.5, 37.7, -122.4, 37.8)]
        )
        assert crs_less_gdf.crs is None
        geojson_path = tmp_path / "crs-less.geojson"
        crs_less_gdf.to_file(geojson_path, driver="GeoJSON")

        # Stub fetch_geospatial to return our CRS-less path without HTTP.
        import muni_walk_access.ingest.sources.datasf as _datasf_mod

        monkeypatch.setattr(
            _datasf_mod, "fetch_geospatial", lambda *a, **k: geojson_path
        )

        cfg: Config = load_config(_CONFIG_PATH).model_copy(update={})
        ctx = RunContext.from_config(
            run_id="test",
            config=cfg,
            cache=CacheManager(root=tmp_path, ttl_days=30),
        )
        lens = LensConfig(id="x", datasf_id="abcd-1234", label="X")
        out = DataSFBoundarySource().fetch(lens, ctx)
        assert out.crs is not None
        assert out.crs.to_epsg() == 4326
