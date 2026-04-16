"""Data contract Pydantic schemas for the muni-walk-access pipeline."""

from __future__ import annotations

from pydantic import BaseModel, field_validator, model_validator


def _validate_pct_matrix(v: list[list[float]]) -> list[list[float]]:
    """Assert every cell in a pct_within matrix is in [0.0, 1.0]."""
    for row in v:
        for val in row:
            if not (0.0 <= val <= 1.0):
                raise ValueError(f"pct_within values must be in [0.0, 1.0], got {val}")
    return v


class GridAxes(BaseModel):
    """Frequency and walking axes that index the accessibility grid."""

    frequency_minutes: list[int]
    walking_minutes: list[int]

    @field_validator("frequency_minutes", "walking_minutes", mode="after")
    @classmethod
    def must_be_sorted_ascending(cls, v: list[int]) -> list[int]:
        """Raise if axis values are not sorted in ascending order."""
        if v != sorted(v):
            raise ValueError("axis values must be sorted ascending")
        return v


class GridDefaults(BaseModel):
    """Default axis indices used when no user selection is active."""

    frequency_idx: int
    walking_idx: int


class LensFlags(BaseModel):
    """Boolean equity-lens membership flags for a neighbourhood."""

    analysis_neighborhoods: bool
    ej_communities: bool
    equity_strategy: bool


class CityWide(BaseModel):
    """City-wide aggregate pct_within accessibility matrix."""

    pct_within: list[list[float]]

    @field_validator("pct_within", mode="after")
    @classmethod
    def pct_within_in_range(cls, v: list[list[float]]) -> list[list[float]]:
        """Raise if any pct_within value is outside [0.0, 1.0]."""
        return _validate_pct_matrix(v)


class NeighborhoodGrid(BaseModel):
    """Per-neighbourhood accessibility data and metadata."""

    id: str
    name: str
    population: int
    lens_flags: LensFlags
    pct_within: list[list[float]]

    @field_validator("pct_within", mode="after")
    @classmethod
    def pct_within_in_range(cls, v: list[list[float]]) -> list[list[float]]:
        """Raise if any pct_within value is outside [0.0, 1.0]."""
        return _validate_pct_matrix(v)


class GridSchema(BaseModel):
    """Root schema for the grid.json data contract file."""

    version: str
    run_id: str
    config_snapshot_url: str
    route_mode: str | None = None
    axes: GridAxes
    defaults: GridDefaults
    city_wide: CityWide
    neighborhoods: list[NeighborhoodGrid]

    @model_validator(mode="after")
    def validate_grid_structure(self) -> GridSchema:
        """Validate matrix dimensions match axes and defaults are in bounds."""
        n_freq = len(self.axes.frequency_minutes)
        n_walk = len(self.axes.walking_minutes)
        if not (0 <= self.defaults.frequency_idx < n_freq):
            raise ValueError(
                f"defaults.frequency_idx {self.defaults.frequency_idx}"
                f" out of bounds for {n_freq} frequency values"
            )
        if not (0 <= self.defaults.walking_idx < n_walk):
            raise ValueError(
                f"defaults.walking_idx {self.defaults.walking_idx}"
                f" out of bounds for {n_walk} walking values"
            )
        matrices = [
            ("city_wide", self.city_wide.pct_within),
            *((f"neighborhood '{nb.id}'", nb.pct_within) for nb in self.neighborhoods),
        ]
        for label, matrix in matrices:
            if len(matrix) != n_freq:
                raise ValueError(
                    f"{label} pct_within has {len(matrix)} rows, expected {n_freq}"
                )
            for i, row in enumerate(matrix):
                if len(row) != n_walk:
                    raise ValueError(
                        f"{label} pct_within[{i}] has"
                        f" {len(row)} cols, expected {n_walk}"
                    )
        return self


class CodeVersion(BaseModel):
    """Git coordinates of the pipeline build that produced an artifact."""

    git_sha: str
    git_tag: str


class DataVersions(BaseModel):
    """Upstream data source versions used in a pipeline run."""

    gtfs_feed_sha256: str
    gtfs_feed_date: str
    osm_extract_date: str
    datasf_timestamps: dict[str, str]
    datasf_data_updated: dict[str, str]


class ConfigSnapshot(BaseModel):
    """Full pipeline configuration snapshot for run reproducibility."""

    run_id: str
    code_version: CodeVersion
    config_hash: str
    data_versions: DataVersions
    config_values: dict[str, object]
    upstream_fallback: bool


class GroundTruth(BaseModel):
    """Ground-truth validation metrics from manual spot-checks."""

    sample_size: int
    within_10pct: float
    within_20pct: float
    median_error_pct: float
    worst_case_pct: float

    @field_validator(
        "within_10pct",
        "within_20pct",
        "median_error_pct",
        "worst_case_pct",
        mode="after",
    )
    @classmethod
    def pct_in_unit_range(cls, v: float) -> float:
        """Raise if percentage value is outside [0.0, 1.0]."""
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"percentage must be in [0.0, 1.0], got {v}")
        return v


class ComparisonTool(BaseModel):
    """Optional comparison result against an external routing tool."""

    name: str
    pct_agreement: float

    @field_validator("pct_agreement", mode="after")
    @classmethod
    def pct_in_unit_range(cls, v: float) -> float:
        """Raise if pct_agreement is outside [0.0, 1.0]."""
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"pct_agreement must be in [0.0, 1.0], got {v}")
        return v


class ValidationResults(BaseModel):
    """Pipeline validation results against ground-truth data."""

    run_id: str
    ground_truth: GroundTruth
    comparison_tool: ComparisonTool | None = None


class NeighborhoodFeatureProperties(BaseModel):
    """Properties on each feature in the neighborhoods.geojson file."""

    id: str
    name: str
    population: int
    lens_flags: LensFlags
    pct_at_defaults: float

    @field_validator("pct_at_defaults", mode="after")
    @classmethod
    def pct_in_unit_range(cls, v: float) -> float:
        """Raise if pct_at_defaults is outside [0.0, 1.0]."""
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"pct_at_defaults must be in [0.0, 1.0], got {v}")
        return v


class HexCell(BaseModel):
    """Per-hex-cell accessibility data for an H3 resolution."""

    id: str
    center_lat: float
    center_lon: float
    population: int
    pct_within: list[list[float]]

    @field_validator("pct_within", mode="after")
    @classmethod
    def pct_within_in_range(cls, v: list[list[float]]) -> list[list[float]]:
        """Raise if any pct_within value is outside [0.0, 1.0]."""
        return _validate_pct_matrix(v)


class HexGridSchema(BaseModel):
    """Root schema for the grid_hex.json data contract file."""

    version: str
    h3_resolution: int
    run_id: str
    config_snapshot_url: str
    time_window: str | None = None
    route_mode: str | None = None
    axes: GridAxes
    defaults: GridDefaults
    cells: list[HexCell]

    @model_validator(mode="after")
    def validate_hex_grid_structure(self) -> "HexGridSchema":
        """Validate matrix dimensions match axes and defaults are in bounds."""
        n_freq = len(self.axes.frequency_minutes)
        n_walk = len(self.axes.walking_minutes)
        if not (0 <= self.defaults.frequency_idx < n_freq):
            raise ValueError(
                f"defaults.frequency_idx {self.defaults.frequency_idx}"
                f" out of bounds for {n_freq} frequency values"
            )
        if not (0 <= self.defaults.walking_idx < n_walk):
            raise ValueError(
                f"defaults.walking_idx {self.defaults.walking_idx}"
                f" out of bounds for {n_walk} walking values"
            )
        for cell in self.cells:
            if len(cell.pct_within) != n_freq:
                raise ValueError(
                    f"Cell '{cell.id}' pct_within has {len(cell.pct_within)} rows,"
                    f" expected {n_freq}"
                )
            for i, row in enumerate(cell.pct_within):
                if len(row) != n_walk:
                    raise ValueError(
                        f"Cell '{cell.id}' pct_within[{i}] has"
                        f" {len(row)} cols, expected {n_walk}"
                    )
        return self
