"""Grid JSON emitter for the muni-walk-access data contract."""

from __future__ import annotations

import logging
from pathlib import Path

from muni_walk_access.config import Config
from muni_walk_access.emit.schemas import (
    CityWide,
    GridAxes,
    GridDefaults,
    GridSchema,
    NeighborhoodGrid,
)

logger = logging.getLogger(__name__)


def write_grid_json(
    neighborhoods: list[NeighborhoodGrid],
    city_wide: CityWide,
    config: Config,
    run_id: str,
    output_dir: Path,
) -> Path:
    """Write grid.json to {output_dir}/site/src/data/grid.json.

    Returns the path to the written file.
    """
    if not neighborhoods:
        raise ValueError("neighborhoods must not be empty")

    freq_idx = config.grid.frequency_threshold_min.index(
        config.grid.defaults.frequency_min
    )
    walk_idx = config.grid.walking_minutes.index(config.grid.defaults.walking_min)

    schema = GridSchema(
        version="1.0.0",
        run_id=run_id,
        config_snapshot_url="./config_snapshot.json",
        axes=GridAxes(
            frequency_minutes=config.grid.frequency_threshold_min,
            walking_minutes=config.grid.walking_minutes,
        ),
        defaults=GridDefaults(frequency_idx=freq_idx, walking_idx=walk_idx),
        city_wide=city_wide,
        neighborhoods=sorted(neighborhoods, key=lambda n: n.id),
    )

    out_dir = output_dir / "site" / "src" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "grid.json"
    out_path.write_text(schema.model_dump_json(indent=2))
    logger.info("Grid JSON written: %s", out_path)
    return out_path
