"""Tests for data contract schemas (Story 1.2)."""

from __future__ import annotations

import json
import pathlib

import pytest
from pydantic import ValidationError

from muni_walk_access.emit.schemas import (
    ConfigSnapshot,
    GridSchema,
    ValidationResults,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


class TestGridSchemaRoundTrip:
    """Round-trip and field validation for GridSchema."""

    def test_parse_sample_fixture(self) -> None:
        """Verify GridSchema parses the sample fixture without error."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        grid = GridSchema.model_validate_json(raw)
        assert grid.version == "1.0.0"
        assert grid.run_id == "2026-04-01T02:15:00Z"

    def test_round_trip(self) -> None:
        """Verify that serializing then re-parsing produces equal models."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        grid1 = GridSchema.model_validate_json(raw)
        serialized = grid1.model_dump_json()
        grid2 = GridSchema.model_validate_json(serialized)
        assert grid1 == grid2

    def test_neighborhood_fields(self) -> None:
        """Verify neighbourhood id, population, and lens_flags are correct."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        grid = GridSchema.model_validate_json(raw)
        nbhd = grid.neighborhoods[0]
        assert nbhd.id == "outer-mission"
        assert nbhd.population == 21430
        assert nbhd.lens_flags.equity_strategy is True
        assert nbhd.lens_flags.ej_communities is False

    def test_pct_within_dimensions(self) -> None:
        """Verify pct_within matrix dimensions match axes lengths."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        grid = GridSchema.model_validate_json(raw)
        n_freq = len(grid.axes.frequency_minutes)
        n_walk = len(grid.axes.walking_minutes)
        assert len(grid.city_wide.pct_within) == n_freq
        assert all(len(row) == n_walk for row in grid.city_wide.pct_within)
        nbhd = grid.neighborhoods[0]
        assert len(nbhd.pct_within) == n_freq
        assert all(len(row) == n_walk for row in nbhd.pct_within)


class TestGridSchemaRejection:
    """Verify GridSchema rejects invalid data with ValidationError."""

    def test_unsorted_frequency_axes(self) -> None:
        """Verify unsorted frequency_minutes raises ValidationError."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        data = json.loads(raw)
        data["axes"]["frequency_minutes"] = [10, 4, 6, 8, 12, 15, 20]
        with pytest.raises(ValidationError):
            GridSchema.model_validate(data)

    def test_unsorted_walking_axes(self) -> None:
        """Verify unsorted walking_minutes raises ValidationError."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        data = json.loads(raw)
        data["axes"]["walking_minutes"] = [15, 3, 5, 7, 10, 12]
        with pytest.raises(ValidationError):
            GridSchema.model_validate(data)

    def test_city_wide_pct_above_one(self) -> None:
        """Verify city_wide pct_within > 1.0 raises ValidationError."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        data = json.loads(raw)
        data["city_wide"]["pct_within"][0][0] = 1.5
        with pytest.raises(ValidationError):
            GridSchema.model_validate(data)

    def test_neighborhood_pct_below_zero(self) -> None:
        """Verify neighbourhood pct_within < 0.0 raises ValidationError."""
        raw = (FIXTURES / "sample_grid.json").read_text()
        data = json.loads(raw)
        data["neighborhoods"][0]["pct_within"][0][0] = -0.1
        with pytest.raises(ValidationError):
            GridSchema.model_validate(data)

    def test_missing_required_field(self) -> None:
        """Verify missing required top-level field raises ValidationError."""
        with pytest.raises(ValidationError):
            GridSchema.model_validate({"version": "1.0.0"})


class TestValidationResultsSchema:
    """Validate ValidationResults schema including nested ground_truth."""

    VALID: dict[str, object] = {
        "run_id": "2026-04-01T02:15:00Z",
        "ground_truth": {
            "sample_size": 200,
            "within_10pct": 0.78,
            "within_20pct": 0.92,
            "median_error_pct": 0.08,
            "worst_case_pct": 0.31,
        },
    }

    def test_valid_without_comparison_tool(self) -> None:
        """Verify ValidationResults parses with comparison_tool absent."""
        result = ValidationResults.model_validate(self.VALID)
        assert result.comparison_tool is None
        assert result.ground_truth.sample_size == 200

    def test_valid_with_comparison_tool(self) -> None:
        """Verify ValidationResults parses with optional comparison_tool."""
        data = {
            **self.VALID,
            "comparison_tool": {"name": "511.org", "pct_agreement": 0.91},
        }
        result = ValidationResults.model_validate(data)
        assert result.comparison_tool is not None
        assert result.comparison_tool.name == "511.org"

    def test_flat_ground_truth_rejected(self) -> None:
        """Verify flat ground_truth fields (not nested) raise ValidationError."""
        flat: dict[str, object] = {
            "run_id": "2026-04-01T02:15:00Z",
            "sample_size": 200,
            "within_10pct": 0.78,
            "within_20pct": 0.92,
            "median_error_pct": 0.08,
            "worst_case_pct": 0.31,
        }
        with pytest.raises(ValidationError):
            ValidationResults.model_validate(flat)


class TestConfigSnapshotSchema:
    """Validate ConfigSnapshot schema including nested sub-models."""

    VALID: dict[str, object] = {
        "run_id": "2026-04-01T02:15:00Z",
        "code_version": {"git_sha": "abc123def", "git_tag": "v0.1.0"},
        "config_hash": "sha256:deadbeef",
        "data_versions": {
            "gtfs_feed_sha256": "sha256:cafebabe",
            "osm_extract_date": "2026-03-15",
            "datasf_timestamps": {"neighborhoods": "2026-01-10T00:00:00Z"},
        },
        "config_values": {"walk_speed_kmh": 4.8},
        "upstream_fallback": False,
    }

    def test_valid_config_snapshot(self) -> None:
        """Verify ConfigSnapshot parses a valid inline fixture."""
        snap = ConfigSnapshot.model_validate(self.VALID)
        assert snap.run_id == "2026-04-01T02:15:00Z"
        assert snap.code_version.git_sha == "abc123def"
        assert snap.upstream_fallback is False

    def test_nested_data_versions(self) -> None:
        """Verify nested DataVersions fields are accessible."""
        snap = ConfigSnapshot.model_validate(self.VALID)
        assert snap.data_versions.osm_extract_date == "2026-03-15"
        assert "neighborhoods" in snap.data_versions.datasf_timestamps

    def test_missing_upstream_fallback_rejected(self) -> None:
        """Verify missing upstream_fallback raises ValidationError."""
        data = {k: v for k, v in self.VALID.items() if k != "upstream_fallback"}
        with pytest.raises(ValidationError):
            ConfigSnapshot.model_validate(data)
