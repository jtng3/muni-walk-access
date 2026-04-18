"""Generate residential spot-check fixture for Story 1.6 AC-3.

Run from the pipeline/ directory:

    uv run python tests/generate_residential_spot_check.py

Requires network access to DataSF (or a warm .cache/). First run fetches
~3.7M rows from wv5m-vpq2 (Assessor Tax Rolls) — expect several minutes.
Subsequent runs use the local Parquet cache and are fast.

Writes pipeline/tests/fixtures/residential_spot_check.yaml with 20 randomly
sampled residential addresses and Google Street View URLs. After running:

1. Open each street_view_url and verify the address is residential.
2. Fill in ``finding`` (residential / non-residential) and ``verdict``
   (correct / incorrect) for every entry.
3. Confirm >=19/20 entries are correct, then commit the file.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

# Ensure src/ is on the path when run directly.
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from muni_walk_access.config import load_config  # noqa: E402
from muni_walk_access.ingest.cache import CacheManager  # noqa: E402
from muni_walk_access.ingest.sources.datasf import (  # noqa: E402
    fetch_residential_addresses,
)
from muni_walk_access.run_context import RunContext  # noqa: E402

_SEED = 42
_SAMPLE_SIZE = 20
_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "residential_spot_check.yaml"
_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _street_view_url(lat: float, lon: float) -> str:
    """Return a Google Maps Street View URL using the Maps URLs API."""
    return f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lon}"


def main() -> None:
    """Fetch residential addresses, sample 20, write spot-check YAML."""
    config = load_config(_CONFIG_PATH)
    cache = CacheManager(
        root=config.ingest.cache_dir, ttl_days=config.ingest.cache_ttl_days
    )
    ctx = RunContext.from_config(run_id="spot-check", config=config, cache=cache)

    print("Fetching residential addresses from DataSF (slow on first run)…")
    df = fetch_residential_addresses(config, ctx=ctx)

    if df.is_empty():
        print(
            "ERROR: fetch_residential_addresses() returned 0 rows.\n"
            "  • Check that config.yaml use_codes_residential matches the actual\n"
            "    use_code values in the parcel dataset.\n"
            "  • Verify that parcel_number formats match between EAS and the parcel\n"
            "    dataset — a format mismatch silently produces 0 join hits.\n"
            "  See pipeline/docs/residential-filter-spike.md for details."
        )
        sys.exit(1)

    n = len(df)
    print(f"Got {n} residential addresses. Sampling {_SAMPLE_SIZE} (seed={_SEED})…")
    sample = df.sample(n=_SAMPLE_SIZE, seed=_SEED, shuffle=True)

    entries = []
    for row in sample.iter_rows(named=True):
        address = str(row.get("address", ""))
        lat = float(row.get("latitude", 0.0))
        lon = float(row.get("longitude", 0.0))
        use_code = str(row.get("use_code", ""))
        entries.append(
            {
                "address": address,
                "lat": lat,
                "lon": lon,
                "use_code": use_code,
                "street_view_url": _street_view_url(lat, lon),
                "finding": "TBD",  # Fill in: "residential" or "non-residential"
                "verdict": "TBD",  # Fill in: "correct" or "incorrect"
            }
        )

    data = {
        "metadata": {
            "dataset": (
                "3mea-di5p (EAS) inner-joined with wv5m-vpq2 (Assessor Tax Rolls)"
            ),
            "sample_seed": _SEED,
            "sample_size": _SAMPLE_SIZE,
            "total_pool": len(df),
            "instructions": (
                "For each entry: open street_view_url and verify whether the address "
                "is residential. Set finding to 'residential' or 'non-residential'. "
                "Set verdict to 'correct' (filter correctly included it) or "
                "'incorrect' (filter included it but it is not residential). "
                "Story 1.6 AC-3 requires >=19/20 (95%) correct."
            ),
        },
        "entries": entries,
    }

    _FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_FIXTURE_PATH, "w") as fh:
        yaml.dump(
            data, fh, default_flow_style=False, allow_unicode=True, sort_keys=False
        )

    print(f"\nWrote {_SAMPLE_SIZE} entries to {_FIXTURE_PATH}")
    print("\n=== ADDRESSES FOR STREET VIEW VERIFICATION ===\n")
    for i, entry in enumerate(entries, 1):
        print(f"{i:2d}. {entry['address']}")
        print(f"    use_code: {entry['use_code']}")
        print(f"    url: {entry['street_view_url']}\n")
    print("=== END — verify above in Street View, then fill in finding/verdict ===")


if __name__ == "__main__":
    main()
