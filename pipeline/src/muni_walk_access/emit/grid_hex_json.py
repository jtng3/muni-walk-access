"""Grid hex JSON emitter for the muni-walk-access data contract."""

from __future__ import annotations

import logging
from pathlib import Path

from muni_walk_access.config import Config
from muni_walk_access.emit.schemas import (
    GridAxes,
    GridDefaults,
    HexCell,
    HexGridSchema,
)

logger = logging.getLogger(__name__)

# Expected cell counts per H3 resolution over SF's land area.
# Warning fires if actual count deviates by more than 2× from expected.
_EXPECTED_CELL_COUNTS: dict[int, int] = {
    4: 5,
    5: 15,
    6: 80,
    7: 300,
    8: 2_000,
    9: 14_000,
    10: 100_000,
}


def write_grid_hex_json(
    hex_grids: dict[int, list[HexCell]],
    config: Config,
    run_id: str,
    output_dir: Path,
) -> list[Path]:
    """Write one grid_hex_r{res}.json per resolution.

    Args:
        hex_grids: Mapping of H3 resolution → cell list (from compute_hex_grids).
        config: Pipeline configuration.
        run_id: Pipeline run identifier.
        output_dir: Repository root (files land in site/src/data/).

    Returns:
        List of paths to the written files.

    Raises:
        ValueError: If hex_grids is empty.

    """
    if not hex_grids:
        raise ValueError("hex_grids must not be empty")

    freq_idx = config.grid.frequency_threshold_min.index(
        config.grid.defaults.frequency_min
    )
    walk_idx = config.grid.walking_minutes.index(config.grid.defaults.walking_min)

    axes = GridAxes(
        frequency_minutes=config.grid.frequency_threshold_min,
        walking_minutes=config.grid.walking_minutes,
    )
    defaults = GridDefaults(frequency_idx=freq_idx, walking_idx=walk_idx)

    out_dir = output_dir / "site" / "src" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for res in sorted(hex_grids.keys()):
        cells = hex_grids[res]
        if not cells:
            logger.warning("Hex grid r%d: no cells — skipping file", res)
            continue

        cell_count = len(cells)
        logger.info("Grid hex r%d: %d cells", res, cell_count)

        expected = _EXPECTED_CELL_COUNTS.get(res)
        if expected is not None and (
            cell_count > expected * 2 or cell_count < expected // 2
        ):
            logger.warning(
                "Hex r%d cell count %d deviates >2× from expected ~%d",
                res,
                cell_count,
                expected,
            )

        schema = HexGridSchema(
            version="1.0.0",
            h3_resolution=res,
            run_id=run_id,
            config_snapshot_url="./config_snapshot.json",
            axes=axes,
            defaults=defaults,
            cells=sorted(cells, key=lambda c: c.id),
        )

        out_path = out_dir / f"grid_hex_r{res}.json"
        out_path.write_text(schema.model_dump_json(indent=2))
        logger.info("Grid hex r%d JSON written: %s", res, out_path)
        written.append(out_path)

    return written
