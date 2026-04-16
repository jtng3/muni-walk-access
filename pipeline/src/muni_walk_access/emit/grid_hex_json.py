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

# Expected *occupied* cell counts per H3 resolution (cells with ≥1 SF address).
# Warning fires if actual count deviates by more than 2× from expected.
# Update these after any significant address-dataset refresh.
_EXPECTED_CELL_COUNTS: dict[int, int] = {
    7: 29,
    8: 161,
    9: 896,
    10: 5_245,
    11: 28_709,
}


def write_grid_hex_json(
    hex_grids: dict[int, list[HexCell]],
    config: Config,
    run_id: str,
    output_dir: Path,
    time_window: str | None = None,
    route_mode: str | None = None,
) -> list[Path]:
    """Write one grid_hex_r{res}[_{time_window}][_{route_mode}].json per resolution.

    Args:
        hex_grids: Mapping of H3 resolution → cell list (from compute_hex_grids).
        config: Pipeline configuration.
        run_id: Pipeline run identifier.
        output_dir: Repository root (files land in site/src/data/).
        time_window: If set, included in schema and filename
            (e.g. ``grid_hex_r9_am_peak.json``).
        route_mode: If set, appended to filename and included in schema
            (e.g. ``grid_hex_r9_am_peak_headway.json``).

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

    suffix = f"_{time_window}" if time_window else ""
    if route_mode:
        suffix = f"{suffix}_{route_mode}"

    written: list[Path] = []
    for res in sorted(hex_grids.keys()):
        cells = hex_grids[res]
        if not cells:
            logger.warning("Hex grid r%d%s: no cells — skipping file", res, suffix)
            continue

        cell_count = len(cells)
        logger.info("Grid hex r%d%s: %d cells", res, suffix, cell_count)

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
            version="2.0.0" if time_window else "1.0.0",
            h3_resolution=res,
            run_id=run_id,
            config_snapshot_url="./config_snapshot.json",
            time_window=time_window,
            route_mode=route_mode,
            axes=axes,
            defaults=defaults,
            cells=sorted(cells, key=lambda c: c.id),
        )

        out_path = out_dir / f"grid_hex_r{res}{suffix}.json"
        out_path.write_text(schema.model_dump_json(indent=2))
        logger.info("Grid hex r%d%s JSON written: %s", res, suffix, out_path)
        written.append(out_path)

    return written
