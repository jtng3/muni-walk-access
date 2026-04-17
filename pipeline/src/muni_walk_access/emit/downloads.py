"""Downloads emitter for the muni-walk-access data contract."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import polars as pl

from muni_walk_access.config import Config
from muni_walk_access.emit.schemas import NeighborhoodGrid

logger = logging.getLogger(__name__)


def write_downloads(
    neighborhoods: list[NeighborhoodGrid],
    stratified: pl.DataFrame,
    config_snapshot_path: Path,
    geojson_path: Path,
    run_id: str,
    output_dir: Path,
    config: Config,
) -> list[Path]:
    """Write download artifacts to {output_dir}/site/public/downloads/.

    Writes four files per run:
    - muni-walk-access-{run_id}-neighborhoods.geojson
    - muni-walk-access-{run_id}-neighborhoods.parquet (one row per neighbourhood)
    - muni-walk-access-{run_id}-addresses.parquet    (full per-address data)
    - muni-walk-access-{run_id}-config-snapshot.json

    Returns list of paths to written files.
    """
    safe_run_id = run_id.replace(":", "-")
    out_dir = output_dir / "site" / "public" / "downloads"
    out_dir.mkdir(parents=True, exist_ok=True)

    freq_idx = config.grid.frequency_threshold_min.index(
        config.grid.defaults.frequency_min
    )
    walk_idx = config.grid.walking_minutes.index(config.grid.defaults.walking_min)

    written: list[Path] = []

    # 1. neighborhoods.geojson — copy from geojson output
    geojson_dest = out_dir / f"muni-walk-access-{safe_run_id}-neighborhoods.geojson"
    shutil.copy(geojson_path, geojson_dest)
    written.append(geojson_dest)

    # 2. neighborhoods.parquet — aggregated neighborhood-level data.
    # Lens columns are derived from the first neighborhood's lens_flags keys
    # (config-declared order). This produces one column per lens — including
    # analysis_neighborhoods, which is always True for SF (it was explicitly
    # excluded pre-5-2). Accepted schema change; documented in Story 5-2.
    lens_keys = list(neighborhoods[0].lens_flags.keys()) if neighborhoods else []
    columns: dict[str, list[object]] = {
        "id": [n.id for n in neighborhoods],
        "name": [n.name for n in neighborhoods],
        "population": [n.population for n in neighborhoods],
    }
    for lens_key in lens_keys:
        columns[lens_key] = [n.lens_flags.get(lens_key, False) for n in neighborhoods]
    columns["pct_at_defaults"] = [
        n.pct_within[freq_idx][walk_idx] for n in neighborhoods
    ]
    nbhd_df = pl.DataFrame(columns)
    nbhd_parquet = out_dir / f"muni-walk-access-{safe_run_id}-neighborhoods.parquet"
    nbhd_df.write_parquet(nbhd_parquet)
    written.append(nbhd_parquet)

    # 3. addresses.parquet — full per-address stratified data
    addr_parquet = out_dir / f"muni-walk-access-{safe_run_id}-addresses.parquet"
    stratified.write_parquet(addr_parquet)
    written.append(addr_parquet)

    # 4. config-snapshot.json — copy from config_snapshot output
    config_dest = out_dir / f"muni-walk-access-{safe_run_id}-config-snapshot.json"
    shutil.copy(config_snapshot_path, config_dest)
    written.append(config_dest)

    logger.info("Download files written to %s (%d files)", out_dir, len(written))
    return written
