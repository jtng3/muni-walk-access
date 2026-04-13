"""Pipeline configuration model loaded from config.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class FrequencyBin(BaseModel):
    """A transit headway classification bin."""

    id: str
    max_headway_min: int | None


class FrequencyConfig(BaseModel):
    """Transit frequency classification parameters."""

    bins: list[FrequencyBin]
    peak_window: str
    peak_am_start: str
    peak_am_end: str


class GridDefaultsConfig(BaseModel):
    """Default axis values (not indices) for the accessibility grid."""

    frequency_min: int
    walking_min: int


class GridConfig(BaseModel):
    """Accessibility grid axis definitions and default values."""

    frequency_threshold_min: list[int]
    walking_minutes: list[int]
    defaults: GridDefaultsConfig

    @field_validator("frequency_threshold_min", "walking_minutes", mode="after")
    @classmethod
    def must_be_strictly_increasing(cls, v: list[int]) -> list[int]:
        """Raise if axis values are not strictly increasing (no duplicates)."""
        if v != sorted(set(v)):
            raise ValueError("axis values must be strictly increasing (no duplicates)")
        return v

    @model_validator(mode="after")
    def defaults_must_be_in_axes(self) -> GridConfig:
        """Validate that default values exist in their respective axes."""
        if self.defaults.frequency_min not in self.frequency_threshold_min:
            raise ValueError(
                f"defaults.frequency_min {self.defaults.frequency_min}"
                f" not in frequency_threshold_min {self.frequency_threshold_min}"
            )
        if self.defaults.walking_min not in self.walking_minutes:
            raise ValueError(
                f"defaults.walking_min {self.defaults.walking_min}"
                f" not in walking_minutes {self.walking_minutes}"
            )
        return self


class IngestConfig(BaseModel):
    """Data ingestion caching parameters."""

    cache_ttl_days: int = Field(default=30, gt=0)
    cache_dir: Path = Path(".cache")


class WalkingConfig(BaseModel):
    """Walking speed parameters."""

    pace_min_per_mile: float = Field(gt=0)


class ResidentialFilterConfig(BaseModel):
    """Residential parcel filter configuration.

    Note: parcel_dataset_id is a known blocker for FR5 (residential filter).
    The placeholder 'TBD_FROM_LUKE' must be replaced with the actual DataSF
    parcel dataset identifier confirmed by Luke Armbruster (SF MUNI).
    """

    parcel_dataset_id: str = Field(
        description=(
            "DataSF parcel dataset ID. KNOWN BLOCKER for FR5: value"
            " 'TBD_FROM_LUKE' is a placeholder pending confirmation"
            " from Luke Armbruster (SF MUNI)."
        )
    )
    use_codes_residential: list[str]


class LensConfig(BaseModel):
    """An equity lens geographic dataset."""

    id: str
    datasf_id: str
    label: str


class ValidationConfig(BaseModel):
    """Pipeline output validation parameters."""

    ground_truth_file: str
    pass_threshold: float

    @field_validator("pass_threshold", mode="after")
    @classmethod
    def pass_threshold_in_range(cls, v: float) -> float:
        """Raise if pass_threshold is outside [0.0, 1.0]."""
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"pass_threshold must be in [0.0, 1.0], got {v}")
        return v


class DevConfig(BaseModel):
    """Developer ergonomics configuration."""

    sample_size: int | None = Field(None, gt=0)


class Config(BaseModel):
    """Root pipeline configuration model."""

    version: str
    frequency: FrequencyConfig
    grid: GridConfig
    walking: WalkingConfig
    residential_filter: ResidentialFilterConfig
    lenses: list[LensConfig]
    validation: ValidationConfig
    dev: DevConfig
    ingest: IngestConfig = IngestConfig()


def load_config(path: Path) -> Config:
    """Load and validate pipeline configuration from a YAML file.

    Args:
        path: Path to the config.yaml file.

    Returns:
        Validated Config instance.

    Raises:
        FileNotFoundError: If the config file does not exist.
        pydantic.ValidationError: If the config fails Pydantic validation.

    """
    data: Any = yaml.safe_load(path.read_text())
    if data is None:
        msg = f"Config file is empty: {path}"
        raise ValueError(msg)
    return Config.model_validate(data)
