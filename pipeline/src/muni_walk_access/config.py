"""Pipeline configuration model loaded from config.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class FrequencyBin(BaseModel):
    """A transit headway classification bin."""

    id: str
    max_headway_min: int | None


class TimeWindow(BaseModel):
    """A named time-of-day window for frequency analysis."""

    key: str
    label: str
    start: str  # "HH:MM"
    end: str  # "HH:MM"

    @field_validator("start", "end", mode="after")
    @classmethod
    def must_be_hhmm(cls, v: str) -> str:
        """Reject malformed time strings at config load."""
        parts = v.split(":")
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            raise ValueError(f"Time must be HH:MM, got {v!r}")
        h, m = int(parts[0]), int(parts[1])
        if h > 23 or m > 59:
            raise ValueError(f"Time out of range: {v!r}")
        return v

    @model_validator(mode="after")
    def start_must_differ_from_end(self) -> TimeWindow:
        """Reject zero-length windows (start == end matches all departures)."""
        if self.start == self.end:
            raise ValueError(f"start and end must differ (got {self.start!r} for both)")
        return self

    @property
    def start_seconds(self) -> int:
        """Start time as seconds since midnight."""
        h, m = self.start.split(":")
        return int(h) * 3600 + int(m) * 60

    @property
    def end_seconds(self) -> int:
        """End time as seconds since midnight."""
        h, m = self.end.split(":")
        return int(h) * 3600 + int(m) * 60

    @property
    def duration_hours(self) -> float:
        """Window duration in hours (handles overnight wrap)."""
        s, e = self.start_seconds, self.end_seconds
        if e > s:
            return (e - s) / 3600.0
        return (86400 - s + e) / 3600.0


class FrequencyConfig(BaseModel):
    """Transit frequency classification parameters."""

    bins: list[FrequencyBin]
    peak_window: str
    peak_am_start: str
    peak_am_end: str
    service_days: str = "weekday"  # "weekday", "saturday", or "sunday"
    time_windows: list[TimeWindow] = []

    @field_validator("time_windows", mode="after")
    @classmethod
    def unique_window_keys(cls, v: list[TimeWindow]) -> list[TimeWindow]:
        """Reject duplicate time window keys."""
        keys = [tw.key for tw in v]
        if len(keys) != len(set(keys)):
            dupes = [k for k in keys if keys.count(k) > 1]
            raise ValueError(f"Duplicate time_window keys: {sorted(set(dupes))}")
        return v


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


class RoutingConfig(BaseModel):
    """Routing search parameters."""

    max_distance_m: float = Field(
        default=5000.0,
        gt=0,
        description=(
            "Pandana search radius for nearest-stop routing. Addresses whose "
            "nearest stop lies beyond this are treated as unreachable "
            "(null distance / stop_id). Tune per city density."
        ),
    )


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
    """An equity lens geographic dataset.

    Optional per-city filter metadata (Story 5.3) lets a generic filter
    engine drive lens filtering from config alone instead of code branching
    on `lens.id`. SF's existing `config.yaml` continues to load — every
    new field defaults to a safe pass-through value.
    """

    id: str
    datasf_id: str
    label: str

    source_kind: Literal["datasf", "arcgis_hub", "generic_url"] = "datasf"
    name_field: str = "nhood"
    filter_field: str | None = None
    filter_op: Literal["eq", "ne", "gte", "lte", "in"] | None = None
    filter_value: str | int | float | list[str] | None = None
    score_field: str | None = None
    score_threshold: float | None = None

    @model_validator(mode="after")
    def filter_field_and_op_must_pair(self) -> LensConfig:
        """Reject half-configured filters (field without op, or op without field).

        Also rejects orphan ``filter_value`` (set without field+op), since a
        value with no column to apply it to is silently inert and confusing.
        """
        if (self.filter_field is None) != (self.filter_op is None):
            raise ValueError(
                "filter_field and filter_op must be set together "
                f"(got field={self.filter_field!r}, op={self.filter_op!r})"
            )
        if self.filter_value is not None and self.filter_field is None:
            raise ValueError(
                "filter_value requires filter_field + filter_op to be set "
                f"(got value={self.filter_value!r}, field=None)"
            )
        if (self.score_field is None) != (self.score_threshold is None):
            raise ValueError(
                "score_field and score_threshold must be set together "
                f"(got field={self.score_field!r}, "
                f"threshold={self.score_threshold!r})"
            )
        return self


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


class NetworksConfig(BaseModel):
    """Network building parameters."""

    osm_place: str = "San Francisco, California, USA"
    osm_network_type: str = "walk"


class DevConfig(BaseModel):
    """Developer ergonomics configuration."""

    sample_size: int | None = Field(None, gt=0)


class AddressSourceConfig(BaseModel):
    """Which `AddressSource` adapter provides the city's residential addresses.

    Story 5.3: generalizes the previously hardcoded DataSF + EAS assumption
    so Philly (5.4) can ship an OPA Carto adapter without core-module edits.

    ``kind`` is typed ``str`` rather than ``Literal["datasf", ...]`` so new
    adapters can register themselves into the factory (via
    :func:`ingest.sources.get_address_source`) without touching this file.
    The runtime check happens at the factory: an unknown kind raises
    ``KeyError`` with the known-kinds set, which surfaces at pipeline
    startup — no code path silently falls back.
    """

    kind: str = "datasf"


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
    networks: NetworksConfig = NetworksConfig()
    routing: RoutingConfig = RoutingConfig()
    address_source: AddressSourceConfig = AddressSourceConfig()


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
