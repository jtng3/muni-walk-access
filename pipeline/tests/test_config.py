"""Tests for pipeline configuration model — Story 1.3."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from pydantic import ValidationError

from muni_walk_access.config import Config, load_config

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _raw() -> dict[str, Any]:
    """Load raw YAML config as a Python dict for mutation in tests."""
    return cast(dict[str, Any], yaml.safe_load(CONFIG_PATH.read_text()))


class TestConfigParsing:
    """Verify Config parses the canonical config.yaml correctly (AC-1, AC-3)."""

    def test_valid_config_parses(self) -> None:
        """Verify load_config returns a Config with expected top-level values."""
        config = load_config(CONFIG_PATH)
        assert config.version == "1.0"
        assert config.dev.sample_size is None
        assert config.grid.defaults.frequency_min == 10
        assert config.grid.defaults.walking_min == 5
        assert config.residential_filter.parcel_dataset_id == "TBD_FROM_LUKE"
        assert len(config.lenses) == 3
        assert config.frequency.bins[-1].max_headway_min is None

    def test_round_trip(self) -> None:
        """Verify load + model_dump + model_validate produces equal Config."""
        config1 = load_config(CONFIG_PATH)
        dumped = config1.model_dump()
        config2 = Config.model_validate(dumped)
        assert config1 == config2


class TestConfigRejection:
    """Verify Config rejects invalid input with ValidationError (AC-2)."""

    def test_missing_required_field_raises(self) -> None:
        """Verify removing frequency.bins raises ValidationError."""
        data = _raw()
        del data["frequency"]["bins"]
        with pytest.raises(ValidationError, match="bins"):
            Config.model_validate(data)

    def test_wrong_type_raises_validation_error(self) -> None:
        """Verify non-integer dev.sample_size raises ValidationError."""
        data = _raw()
        data["dev"]["sample_size"] = "abc"
        with pytest.raises(ValidationError, match="sample_size"):
            Config.model_validate(data)


class TestGridAxisValidation:
    """Verify strictly-increasing and cross-field validators on GridConfig."""

    def test_unsorted_frequency_threshold_rejected(self) -> None:
        """Verify unsorted frequency_threshold_min raises ValidationError."""
        data = _raw()
        data["grid"]["frequency_threshold_min"] = [10, 4, 6, 8, 12, 15, 20]
        with pytest.raises(ValidationError):
            Config.model_validate(data)

    def test_duplicate_walking_minutes_rejected(self) -> None:
        """Verify duplicate walking_minutes values raise ValidationError."""
        data = _raw()
        data["grid"]["walking_minutes"] = [3, 5, 5, 10, 12, 15]
        with pytest.raises(ValidationError):
            Config.model_validate(data)

    def test_defaults_frequency_min_not_in_axis_rejected(self) -> None:
        """Verify defaults.frequency_min absent from axis raises ValidationError."""
        data = _raw()
        data["grid"]["defaults"]["frequency_min"] = 9
        with pytest.raises(ValidationError):
            Config.model_validate(data)

    def test_defaults_walking_min_not_in_axis_rejected(self) -> None:
        """Verify defaults.walking_min absent from axis raises ValidationError."""
        data = _raw()
        data["grid"]["defaults"]["walking_min"] = 4
        with pytest.raises(ValidationError):
            Config.model_validate(data)


class TestValidationThresholdRejection:
    """Verify pass_threshold [0.0, 1.0] enforcement (AC-1)."""

    def test_pass_threshold_above_one_rejected(self) -> None:
        """Verify pass_threshold > 1.0 raises ValidationError."""
        data = _raw()
        data["validation"]["pass_threshold"] = 1.5
        with pytest.raises(ValidationError, match="pass_threshold"):
            Config.model_validate(data)

    def test_pass_threshold_below_zero_rejected(self) -> None:
        """Verify pass_threshold < 0.0 raises ValidationError."""
        data = _raw()
        data["validation"]["pass_threshold"] = -0.1
        with pytest.raises(ValidationError):
            Config.model_validate(data)
