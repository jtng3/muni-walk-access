"""Scaffold smoke tests — verify package is importable and CLI entry point works."""

import subprocess
import sys
from importlib import import_module


def test_package_importable() -> None:
    """muni_walk_access must be importable (validates src layout + editable install)."""
    mod = import_module("muni_walk_access")
    assert mod.__version__ == "0.1.0"


def test_cli_help_exits_zero() -> None:
    """Python -m muni_walk_access --help must exit 0 (AC-6)."""
    result = subprocess.run(
        [sys.executable, "-m", "muni_walk_access", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "muni-walk-access" in result.stdout


def test_cli_sample_flag_exits_zero() -> None:
    """--sample N must parse without error (used by make pipeline-sample)."""
    result = subprocess.run(
        [sys.executable, "-m", "muni_walk_access", "--sample", "100"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
