"""Entry point for the muni-walk-access pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml
from pydantic import ValidationError

from muni_walk_access.config import load_config
from muni_walk_access.exceptions import IngestError


def main() -> None:
    """Run the pipeline."""
    parser = argparse.ArgumentParser(
        prog="muni-walk-access",
        description=(
            "SF MUNI walkshed accessibility pipeline — computes "
            "transit access scores for all SF residential addresses."
        ),
    )
    parser.add_argument(
        "--sample",
        type=int,
        metavar="N",
        default=None,
        help="Run in sample mode with N addresses (default: full dataset ~200k).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        metavar="PATH",
        help="Path to config.yaml (default: config.yaml).",
    )
    args = parser.parse_args()

    if args.sample is not None and args.sample <= 0:
        parser.error("--sample must be a positive integer")

    try:
        config = load_config(Path(args.config))
    except ValidationError as exc:
        print(f"Config validation failed:\n{exc}", file=sys.stderr)
        sys.exit(1)
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as exc:
        print(f"Config YAML syntax error:\n{exc}", file=sys.stderr)
        sys.exit(1)
    except IngestError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    if args.sample is not None:
        config = config.model_copy(
            update={"dev": config.dev.model_copy(update={"sample_size": args.sample})}
        )


if __name__ == "__main__":
    main()
