"""Tests for emit modules — Story 1.10."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from muni_walk_access.config import Config, load_config
from muni_walk_access.emit.config_snapshot import write_config_snapshot
from muni_walk_access.emit.downloads import write_downloads
from muni_walk_access.emit.geojson import write_neighborhoods_geojson
from muni_walk_access.emit.grid_hex_json import write_grid_hex_json
from muni_walk_access.emit.grid_json import write_grid_json
from muni_walk_access.emit.schemas import (
    CityWide,
    GridSchema,
    HexCell,
    HexGridSchema,
    NeighborhoodGrid,
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


def _make_neighborhoods() -> list[NeighborhoodGrid]:
    """Two minimal NeighborhoodGrid objects."""
    n_freq, n_walk = 7, 6
    grid: list[list[float]] = [[0.25] * n_walk for _ in range(n_freq)]
    return [
        NeighborhoodGrid(
            id="castro",
            name="Castro",
            population=10_000,
            lens_flags={
                "analysis_neighborhoods": True,
                "ej_communities": False,
                "equity_strategy": False,
            },
            pct_within=grid,
        ),
        NeighborhoodGrid(
            id="mission",
            name="Mission",
            population=20_000,
            lens_flags={
                "analysis_neighborhoods": True,
                "ej_communities": True,
                "equity_strategy": True,
            },
            pct_within=grid,
        ),
    ]


def _make_city_wide() -> CityWide:
    n_freq, n_walk = 7, 6
    return CityWide(pct_within=[[0.5] * n_walk for _ in range(n_freq)])


def _make_stratified() -> pl.DataFrame:
    """Minimal per-address stratified DataFrame."""
    return pl.DataFrame(
        {
            "address": ["addr-0", "addr-1"],
            "neighborhood_id": ["mission", "castro"],
            "nearest_stop_distance_m": [100.0, 200.0],
        }
    )


def _write_fake_boundary(cache_dir: Path, dataset_id: str) -> None:
    """Write a fake Analysis Neighborhoods GeoJSON file to the cache."""
    geojson: dict[str, Any] = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-122.5, 37.7],
                            [-122.4, 37.7],
                            [-122.4, 37.8],
                            [-122.5, 37.8],
                            [-122.5, 37.7],
                        ]
                    ],
                },
                "properties": {"nhood": "Mission"},
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-122.4, 37.7],
                            [-122.3, 37.7],
                            [-122.3, 37.8],
                            [-122.4, 37.8],
                            [-122.4, 37.7],
                        ]
                    ],
                },
                "properties": {"nhood": "Castro"},
            },
        ],
    }
    datasf_dir = cache_dir / "datasf"
    datasf_dir.mkdir(parents=True, exist_ok=True)
    today_str = date.today().strftime("%Y%m%d")
    (datasf_dir / f"{dataset_id}-{today_str}.geojson").write_text(json.dumps(geojson))


# ---------------------------------------------------------------------------
# T3: grid_json
# ---------------------------------------------------------------------------


class TestWriteGridJson:
    """Tests for emit.grid_json.write_grid_json."""

    def test_writes_file(self, tmp_path: Path) -> None:
        """write_grid_json creates grid.json in the expected location."""
        cfg = _full_config()
        write_grid_json(
            _make_neighborhoods(), _make_city_wide(), cfg, "run-1", tmp_path
        )
        out = tmp_path / "site" / "src" / "data" / "grid.json"
        assert out.exists(), "grid.json not created"

    def test_valid_grid_schema(self, tmp_path: Path) -> None:
        """grid.json round-trips through GridSchema.model_validate_json."""
        cfg = _full_config()
        out = write_grid_json(
            _make_neighborhoods(), _make_city_wide(), cfg, "run-1", tmp_path
        )
        schema = GridSchema.model_validate_json(out.read_text())
        assert schema.version == "1.0.0"
        assert schema.run_id == "run-1"
        assert len(schema.neighborhoods) == 2

    def test_neighborhoods_sorted_by_id(self, tmp_path: Path) -> None:
        """Neighborhoods in grid.json are sorted by id."""
        cfg = _full_config()
        out = write_grid_json(
            _make_neighborhoods(), _make_city_wide(), cfg, "run-1", tmp_path
        )
        schema = GridSchema.model_validate_json(out.read_text())
        ids = [n.id for n in schema.neighborhoods]
        assert ids == sorted(ids)

    def test_deterministic_output(self, tmp_path: Path) -> None:
        """Calling write_grid_json twice with identical inputs is byte-identical."""
        cfg = _full_config()
        nbhds = _make_neighborhoods()
        cw = _make_city_wide()
        out1 = tmp_path / "run1"
        out2 = tmp_path / "run2"
        p1 = write_grid_json(nbhds, cw, cfg, "run-det", out1)
        p2 = write_grid_json(nbhds, cw, cfg, "run-det", out2)
        assert p1.read_bytes() == p2.read_bytes()

    def test_raises_on_empty_neighborhoods(self, tmp_path: Path) -> None:
        """write_grid_json raises ValueError for empty neighborhoods list."""
        cfg = _full_config()
        with pytest.raises(ValueError, match="empty"):
            write_grid_json([], _make_city_wide(), cfg, "run-1", tmp_path)


# ---------------------------------------------------------------------------
# T4: config_snapshot
# ---------------------------------------------------------------------------


class TestWriteConfigSnapshot:
    """Tests for emit.config_snapshot.write_config_snapshot."""

    def _snapshot_kwargs(self, output_dir: Path) -> dict[str, Any]:
        return dict(
            run_id="run-1",
            git_sha="abc123",
            git_tag="v0.1.0",
            config_hash="deadbeef",
            gtfs_sha256="cafebabe",
            gtfs_feed_date="Wed, 01 Jan 2026 00:00:00 GMT",
            osm_date="20260101",
            datasf_timestamps={"j2bu-swwd": "20260101"},
            datasf_data_updated={"j2bu-swwd": "2026-01-01T00:00:00+00:00"},
            upstream_fallback=False,
            config_values={"version": "1.0", "ingest": {"cache_dir": ".cache"}},
            output_dir=output_dir,
        )

    def test_writes_file(self, tmp_path: Path) -> None:
        """write_config_snapshot creates config_snapshot.json."""
        write_config_snapshot(**self._snapshot_kwargs(tmp_path))
        out = tmp_path / "site" / "src" / "data" / "config_snapshot.json"
        assert out.exists()

    def test_valid_json(self, tmp_path: Path) -> None:
        """config_snapshot.json is parseable JSON with expected fields."""
        write_config_snapshot(**self._snapshot_kwargs(tmp_path))
        out = tmp_path / "site" / "src" / "data" / "config_snapshot.json"
        data = json.loads(out.read_text())
        assert data["run_id"] == "run-1"
        assert data["config_hash"] == "deadbeef"
        assert data["data_versions"]["gtfs_feed_sha256"] == "cafebabe"

    def test_redacts_absolute_cache_dir(self, tmp_path: Path) -> None:
        """Absolute ingest.cache_dir is redacted in config_snapshot.json."""
        kwargs = self._snapshot_kwargs(tmp_path)
        kwargs["config_values"] = {
            "ingest": {"cache_dir": "/home/user/.cache", "cache_ttl_days": 30}
        }
        write_config_snapshot(**kwargs)  # type: ignore[arg-type]
        out = tmp_path / "site" / "src" / "data" / "config_snapshot.json"
        data = json.loads(out.read_text())
        assert data["config_values"]["ingest"]["cache_dir"] == "<redacted>"

    def test_keeps_relative_cache_dir(self, tmp_path: Path) -> None:
        """Relative ingest.cache_dir is preserved in config_snapshot.json."""
        write_config_snapshot(**self._snapshot_kwargs(tmp_path))
        out = tmp_path / "site" / "src" / "data" / "config_snapshot.json"
        data = json.loads(out.read_text())
        assert data["config_values"]["ingest"]["cache_dir"] == ".cache"

    def test_deterministic_output(self, tmp_path: Path) -> None:
        """Calling write_config_snapshot twice with identical inputs is byte-identical."""  # noqa: E501
        kwargs1 = self._snapshot_kwargs(tmp_path / "run1")
        kwargs2 = self._snapshot_kwargs(tmp_path / "run2")
        p1 = write_config_snapshot(**kwargs1)  # type: ignore[arg-type]
        p2 = write_config_snapshot(**kwargs2)  # type: ignore[arg-type]
        assert p1.read_bytes() == p2.read_bytes()


# ---------------------------------------------------------------------------
# T5: geojson
# ---------------------------------------------------------------------------


class TestWriteNeighborhoodsGeojson:
    """Tests for emit.geojson.write_neighborhoods_geojson."""

    def _cfg_with_cache(self, cache_dir: Path) -> Config:
        cfg = _full_config()
        return cfg.model_copy(
            update={"ingest": cfg.ingest.model_copy(update={"cache_dir": cache_dir})}
        )

    def test_writes_file(self, tmp_path: Path) -> None:
        """write_neighborhoods_geojson creates neighborhoods.geojson."""
        cache_dir = tmp_path / ".cache"
        cfg = self._cfg_with_cache(cache_dir)
        _write_fake_boundary(cache_dir, cfg.lenses[0].datasf_id)

        write_neighborhoods_geojson(_make_neighborhoods(), cfg, tmp_path)
        out = tmp_path / "site" / "public" / "data" / "neighborhoods.geojson"
        assert out.exists()

    def test_valid_feature_collection(self, tmp_path: Path) -> None:
        """neighborhoods.geojson is a valid FeatureCollection."""
        cache_dir = tmp_path / ".cache"
        cfg = self._cfg_with_cache(cache_dir)
        _write_fake_boundary(cache_dir, cfg.lenses[0].datasf_id)

        out = write_neighborhoods_geojson(_make_neighborhoods(), cfg, tmp_path)
        data = json.loads(out.read_text())
        assert data["type"] == "FeatureCollection"
        assert isinstance(data["features"], list)
        assert len(data["features"]) == 2

    def test_features_sorted_by_id(self, tmp_path: Path) -> None:
        """Features in neighborhoods.geojson are sorted by properties.id."""
        cache_dir = tmp_path / ".cache"
        cfg = self._cfg_with_cache(cache_dir)
        _write_fake_boundary(cache_dir, cfg.lenses[0].datasf_id)

        out = write_neighborhoods_geojson(_make_neighborhoods(), cfg, tmp_path)
        data = json.loads(out.read_text())
        ids = [f["properties"]["id"] for f in data["features"]]
        assert ids == sorted(ids)

    def test_coordinates_rounded_to_6_decimals(self, tmp_path: Path) -> None:
        """All coordinates are rounded to 6 decimal places."""
        cache_dir = tmp_path / ".cache"
        cfg = self._cfg_with_cache(cache_dir)
        _write_fake_boundary(cache_dir, cfg.lenses[0].datasf_id)

        out = write_neighborhoods_geojson(_make_neighborhoods(), cfg, tmp_path)
        data = json.loads(out.read_text())
        for feature in data["features"]:
            for ring in feature["geometry"]["coordinates"]:
                for coord in ring:
                    for val in coord:
                        s = str(val)
                        decimals = len(s.split(".")[-1]) if "." in s else 0
                        assert decimals <= 6

    def test_raises_on_no_cache(self, tmp_path: Path) -> None:
        """write_neighborhoods_geojson raises ValueError when cache is empty."""
        cache_dir = tmp_path / ".cache"
        cfg = self._cfg_with_cache(cache_dir)
        with pytest.raises(ValueError, match="No cached boundary"):
            write_neighborhoods_geojson(_make_neighborhoods(), cfg, tmp_path)

    def test_deterministic_output(self, tmp_path: Path) -> None:
        """write_neighborhoods_geojson is byte-identical for identical inputs."""
        cache_dir = tmp_path / ".cache"
        cfg = self._cfg_with_cache(cache_dir)
        _write_fake_boundary(cache_dir, cfg.lenses[0].datasf_id)

        out1_dir = tmp_path / "run1"
        out2_dir = tmp_path / "run2"
        p1 = write_neighborhoods_geojson(_make_neighborhoods(), cfg, out1_dir)
        p2 = write_neighborhoods_geojson(_make_neighborhoods(), cfg, out2_dir)
        assert p1.read_bytes() == p2.read_bytes()


# ---------------------------------------------------------------------------
# T6: downloads
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# grid_hex_json (Story 1.11)
# ---------------------------------------------------------------------------


def _make_hex_cells(count: int = 2) -> list[HexCell]:
    """Minimal list of HexCell objects for testing."""
    n_freq, n_walk = 7, 6
    grid: list[list[float]] = [[0.4] * n_walk for _ in range(n_freq)]
    cells = []
    # Use real H3 cell IDs at res 8 near SF
    cell_ids = ["8828308281fffff", "88283082c3fffff"]
    for i in range(count):
        cells.append(
            HexCell(
                id=cell_ids[i % len(cell_ids)],
                center_lat=37.76 + i * 0.01,
                center_lon=-122.42 + i * 0.01,
                population=100 + i * 50,
                pct_within=grid,
            )
        )
    return cells


def _make_hex_grids(resolutions: list[int] | None = None) -> dict[int, list[HexCell]]:
    """Minimal hex_grids dict for testing."""
    if resolutions is None:
        resolutions = [8]
    return {res: _make_hex_cells(2) for res in resolutions}


class TestWriteGridHexJson:
    """Tests for emit.grid_hex_json.write_grid_hex_json."""

    def test_writes_files_per_resolution(self, tmp_path: Path) -> None:
        """write_grid_hex_json creates one grid_hex_r{res}.json per resolution."""
        cfg = _full_config()
        paths = write_grid_hex_json(_make_hex_grids([7, 8]), cfg, "run-1", tmp_path)
        assert len(paths) == 2
        out_dir = tmp_path / "site" / "src" / "data"
        assert (out_dir / "grid_hex_r7.json").exists()
        assert (out_dir / "grid_hex_r8.json").exists()

    def test_valid_hex_grid_schema(self, tmp_path: Path) -> None:
        """grid_hex_r8.json round-trips through HexGridSchema.model_validate_json."""
        cfg = _full_config()
        paths = write_grid_hex_json(_make_hex_grids([8]), cfg, "run-1", tmp_path)
        r8_path = next(p for p in paths if "r8" in p.name)
        schema = HexGridSchema.model_validate_json(r8_path.read_text())
        assert schema.version == "1.0.0"
        assert schema.h3_resolution == 8
        assert schema.run_id == "run-1"
        assert len(schema.cells) == 2

    def test_h3_resolution_field_matches_filename(self, tmp_path: Path) -> None:
        """h3_resolution in each file matches its filename suffix."""
        cfg = _full_config()
        paths = write_grid_hex_json(_make_hex_grids([7, 8]), cfg, "run-1", tmp_path)
        for path in paths:
            schema = HexGridSchema.model_validate_json(path.read_text())
            assert f"r{schema.h3_resolution}" in path.name

    def test_axes_match_grid_json(self, tmp_path: Path) -> None:
        """grid_hex_r8.json axes and defaults match grid.json exactly."""
        cfg = _full_config()
        hex_paths = write_grid_hex_json(_make_hex_grids([8]), cfg, "run-1", tmp_path)
        grid_path = write_grid_json(
            _make_neighborhoods(), _make_city_wide(), cfg, "run-1", tmp_path
        )
        r8_path = next(p for p in hex_paths if "r8" in p.name)
        hex_schema = HexGridSchema.model_validate_json(r8_path.read_text())
        grid_schema = GridSchema.model_validate_json(grid_path.read_text())
        assert hex_schema.axes == grid_schema.axes
        assert hex_schema.defaults.frequency_idx == grid_schema.defaults.frequency_idx
        assert hex_schema.defaults.walking_idx == grid_schema.defaults.walking_idx

    def test_cells_sorted_by_id(self, tmp_path: Path) -> None:
        """Cells in grid_hex_r8.json are sorted by id."""
        cfg = _full_config()
        paths = write_grid_hex_json(_make_hex_grids([8]), cfg, "run-1", tmp_path)
        r8_path = next(p for p in paths if "r8" in p.name)
        schema = HexGridSchema.model_validate_json(r8_path.read_text())
        ids = [c.id for c in schema.cells]
        assert ids == sorted(ids)

    def test_raises_on_empty_hex_grids(self, tmp_path: Path) -> None:
        """write_grid_hex_json raises ValueError for empty hex_grids dict."""
        cfg = _full_config()
        with pytest.raises(ValueError, match="empty"):
            write_grid_hex_json({}, cfg, "run-1", tmp_path)

    def test_deterministic_output(self, tmp_path: Path) -> None:
        """Calling write_grid_hex_json twice with identical inputs is byte-identical."""
        cfg = _full_config()
        hex_grids = _make_hex_grids([8])
        paths1 = write_grid_hex_json(hex_grids, cfg, "run-det", tmp_path / "run1")
        paths2 = write_grid_hex_json(hex_grids, cfg, "run-det", tmp_path / "run2")
        for p1, p2 in zip(
            sorted(paths1, key=lambda p: p.name),
            sorted(paths2, key=lambda p: p.name),
        ):
            assert p1.read_bytes() == p2.read_bytes()

    def test_warns_on_low_cell_count(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warns when cell count deviates >2x from expected."""
        import logging

        cfg = _full_config()
        # 2 cells for res 8 (expected ~2000) → well below expected/2 → should warn
        with caplog.at_level(
            logging.WARNING, logger="muni_walk_access.emit.grid_hex_json"
        ):
            write_grid_hex_json(_make_hex_grids([8]), cfg, "run-1", tmp_path)
        assert any("deviates" in r.message for r in caplog.records)

    def test_skips_resolution_with_no_cells(self, tmp_path: Path) -> None:
        """Resolutions with empty cell lists are skipped (no file written)."""
        cfg = _full_config()
        hex_grids: dict[int, list[HexCell]] = {8: _make_hex_cells(2), 7: []}
        paths = write_grid_hex_json(hex_grids, cfg, "run-1", tmp_path)
        names = [p.name for p in paths]
        assert "grid_hex_r8.json" in names
        assert "grid_hex_r7.json" not in names


class TestWriteDownloads:
    """Tests for emit.downloads.write_downloads."""

    def _write_prerequisites(self, tmp_path: Path) -> tuple[Path, Path]:
        """Write config_snapshot.json and neighborhoods.geojson stubs."""
        snapshot_path = tmp_path / "config_snapshot.json"
        snapshot_path.write_text('{"stub": true}')
        geojson_path = tmp_path / "neighborhoods.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')
        return snapshot_path, geojson_path

    def test_writes_four_files(self, tmp_path: Path) -> None:
        """write_downloads writes exactly 4 download files."""
        snap, geo = self._write_prerequisites(tmp_path)
        cfg = _full_config()
        paths = write_downloads(
            _make_neighborhoods(),
            _make_stratified(),
            snap,
            geo,
            "2026-01-01T00:00:00+00:00",
            tmp_path,
            cfg,
        )
        assert len(paths) == 4
        for p in paths:
            assert p.exists(), f"{p} not created"

    def test_filenames_contain_safe_run_id(self, tmp_path: Path) -> None:
        """Download filenames use a colon-safe run_id."""
        snap, geo = self._write_prerequisites(tmp_path)
        cfg = _full_config()
        paths = write_downloads(
            _make_neighborhoods(),
            _make_stratified(),
            snap,
            geo,
            "2026-01-01T12:00:00+00:00",
            tmp_path,
            cfg,
        )
        for p in paths:
            assert ":" not in p.name

    def test_neighborhoods_parquet_schema(self, tmp_path: Path) -> None:
        """Neighborhoods parquet has expected columns."""
        snap, geo = self._write_prerequisites(tmp_path)
        cfg = _full_config()
        paths = write_downloads(
            _make_neighborhoods(),
            _make_stratified(),
            snap,
            geo,
            "run-1",
            tmp_path,
            cfg,
        )
        nbhd_parquet = next(p for p in paths if "neighborhoods.parquet" in p.name)
        df = pl.read_parquet(nbhd_parquet)
        expected_cols = {
            "id",
            "name",
            "population",
            "ej_communities",
            "equity_strategy",
            "pct_at_defaults",
        }
        assert expected_cols.issubset(set(df.columns))
        assert len(df) == 2

    def test_addresses_parquet_roundtrip(self, tmp_path: Path) -> None:
        """Addresses parquet round-trips to the original stratified DataFrame."""
        snap, geo = self._write_prerequisites(tmp_path)
        cfg = _full_config()
        strat = _make_stratified()
        paths = write_downloads(
            _make_neighborhoods(),
            strat,
            snap,
            geo,
            "run-1",
            tmp_path,
            cfg,
        )
        addr_parquet = next(p for p in paths if "addresses.parquet" in p.name)
        loaded = pl.read_parquet(addr_parquet)
        assert loaded.shape == strat.shape

    def test_deterministic_output(self, tmp_path: Path) -> None:
        """Calling write_downloads twice with identical inputs is byte-identical."""
        snap, geo = self._write_prerequisites(tmp_path)
        cfg = _full_config()
        strat = _make_stratified()
        nbhds = _make_neighborhoods()

        run1 = tmp_path / "run1"
        run2 = tmp_path / "run2"
        snap1 = run1 / "snap.json"
        snap2 = run2 / "snap.json"
        geo1 = run1 / "geo.geojson"
        geo2 = run2 / "geo.geojson"
        for d in (run1, run2):
            d.mkdir()
        snap1.write_bytes(snap.read_bytes())
        snap2.write_bytes(snap.read_bytes())
        geo1.write_bytes(geo.read_bytes())
        geo2.write_bytes(geo.read_bytes())

        paths1 = write_downloads(nbhds, strat, snap1, geo1, "run-det", run1, cfg)
        paths2 = write_downloads(nbhds, strat, snap2, geo2, "run-det", run2, cfg)

        for p1, p2 in zip(
            sorted(paths1, key=lambda p: p.name), sorted(paths2, key=lambda p: p.name)
        ):
            assert p1.read_bytes() == p2.read_bytes(), f"Mismatch for {p1.name}"
