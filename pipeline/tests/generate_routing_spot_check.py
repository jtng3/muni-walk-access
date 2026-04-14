"""Generate routing spot-check fixture for Story 1.7 AC-2.

Run from the pipeline/ directory:

    uv run python tests/generate_routing_spot_check.py

Requires a warm .cache/ (pandana network + DataSF residential addresses + GTFS).
Builds the pandana network, runs sample routing on 1000 addresses (seed=42),
picks 5 address/stop pairs, and writes a YAML with Google Maps walking directions
URLs for manual comparison.

After running:

1. Open each walking_directions_url and note Google Maps' reported walking distance.
2. Fill in ``google_distance_m`` for every entry.
3. Verify >=4/5 computed distances are within 15% of Google's. Set verdict to
   'pass' or 'fail'.
4. Commit pipeline/tests/fixtures/routing_spot_check.yaml once >=4/5 pass.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from muni_walk_access.config import DevConfig, load_config  # noqa: E402
from muni_walk_access.ingest.datasf import fetch_residential_addresses  # noqa: E402
from muni_walk_access.ingest.gtfs import fetch_gtfs  # noqa: E402
from muni_walk_access.network.build import build_network  # noqa: E402
from muni_walk_access.route.nearest_stop import route_nearest_stops  # noqa: E402

_SEED = 42
_SPOT_SAMPLE = 5
_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "routing_spot_check.yaml"
_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _walking_directions_url(
    from_lat: float, from_lon: float, to_lat: float, to_lon: float
) -> str:
    """Return a Google Maps walking directions URL."""
    return (
        f"https://www.google.com/maps/dir/?api=1"
        f"&origin={from_lat},{from_lon}"
        f"&destination={to_lat},{to_lon}"
        f"&travelmode=walking"
    )


def main() -> None:
    """Run sample routing, pick 5 pairs, write spot-check YAML."""
    config = load_config(_CONFIG_PATH)
    config = config.model_copy(update={"dev": DevConfig(sample_size=1000)})

    print("Building pandana network (may use cache)…")
    net, osm_date = build_network(config)

    print("Fetching residential addresses…")
    addresses = fetch_residential_addresses(config)

    print("Fetching GTFS stops…")
    stops_df, _ = fetch_gtfs(config)

    print("Running sample routing (1000 addresses, seed=42)…")
    result = route_nearest_stops(net, addresses, stops_df, config)

    print(f"Routing complete: {len(result)} rows. Sampling {_SPOT_SAMPLE} pairs…")
    sample = result.sample(n=_SPOT_SAMPLE, seed=_SEED)

    # Build a stop_id → (lat, lon) lookup from stops_df
    stop_coords: dict[str, tuple[float, float]] = {}
    for row in stops_df.iter_rows(named=True):
        stop_coords[str(row["stop_id"])] = (
            float(row["stop_lat"]),
            float(row["stop_lon"]),
        )

    entries = []
    for row in sample.iter_rows(named=True):
        stop_id = str(row["nearest_stop_id"])
        from_lat = float(row["latitude"])
        from_lon = float(row["longitude"])
        stop_lat, stop_lon = stop_coords.get(stop_id, (0.0, 0.0))
        dist_m = float(row["nearest_stop_distance_m"])
        entries.append(
            {
                "address": str(row.get("address", "")),
                "from_lat": from_lat,
                "from_lon": from_lon,
                "nearest_stop_id": stop_id,
                "stop_lat": stop_lat,
                "stop_lon": stop_lon,
                "computed_distance_m": round(dist_m, 1),
                "walk_minutes": round(float(row["walk_minutes"]), 2),
                "walking_directions_url": _walking_directions_url(
                    from_lat, from_lon, stop_lat, stop_lon
                ),
                "google_distance_m": "TBD",  # Fill in from Google Maps
                "pct_diff": "TBD",  # Fill in: abs(computed - google) / google * 100
                "verdict": "TBD",  # 'pass' if pct_diff <= 15, else 'fail'
            }
        )

    data = {
        "metadata": {
            "osm_date": osm_date,
            "sample_seed": _SEED,
            "sample_size": len(result),
            "spot_check_size": _SPOT_SAMPLE,
            "pass_threshold": ">=4/5 within 15% of Google Maps walking distance",
            "instructions": (
                "For each entry: open walking_directions_url, note Google's walking "
                "distance in meters (convert from km if needed). "
                "Set google_distance_m. "
                "Compute pct_diff = abs(computed - google) / google * 100. "
                "Set verdict to 'pass' (pct_diff <= 15) or 'fail'. "
                "Story 1.7 AC-2 requires >=4/5 pass."
            ),
        },
        "entries": entries,
    }

    _FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_FIXTURE_PATH, "w") as fh:
        yaml.dump(
            data, fh, default_flow_style=False, allow_unicode=True, sort_keys=False
        )

    print(f"\nWrote {_SPOT_SAMPLE} entries to {_FIXTURE_PATH}")
    print("\n=== PAIRS FOR GOOGLE MAPS VERIFICATION ===\n")
    for i, entry in enumerate(entries, 1):
        print(f"{i}. {entry['address']}")
        dist = entry["computed_distance_m"]
        mins = entry["walk_minutes"]
        print(f"   Computed: {dist}m ({mins} min)")
        sid = entry["nearest_stop_id"]
        slat = entry["stop_lat"]
        slon = entry["stop_lon"]
        print(f"   Stop: {sid} @ ({slat}, {slon})")
        print(f"   URL: {entry['walking_directions_url']}\n")
    print("=== END — fill in google_distance_m + verdict ===")


if __name__ == "__main__":
    main()
