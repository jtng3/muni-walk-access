"""Entry point for the muni-walk-access pipeline."""

import argparse
import sys


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
    parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
