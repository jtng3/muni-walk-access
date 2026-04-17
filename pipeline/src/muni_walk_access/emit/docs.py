"""Pipeline documentation generators for timing and lens verification."""

from __future__ import annotations

import logging
import platform
import sys
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

# Absolute path to pipeline/docs/ — stable regardless of cwd
_DOCS_DIR = Path(__file__).parent.parent.parent.parent / "docs"


def _write_timing_doc(
    *,
    t_network: float,
    t_addresses: float,
    t_gtfs: float,
    t_routing: float,
    t_lens: float,
    t_grid: float,
    t_hex: float,
    t_emit: float,
    t_total: float,
    peak_mb: float,
    address_count: int,
    stop_count: int,
    result_count: int,
    sample_mode: bool,
    sample_n: int | None,
) -> None:
    """Write timing spike markdown to pipeline/docs/timing-spike-{date}.md."""
    today = date.today().strftime("%Y-%m-%d")
    t_min = t_total / 60.0

    if t_min < 20.0:
        verdict = f"PASS: {t_min:.1f} min — within gate threshold (< 20 min)"
    elif t_min < 25.0:
        verdict = (
            f"CAUTION: {t_min:.1f} min — within budget but approaching"
            " limit (20–25 min)"
        )
    else:
        verdict = (
            f"BLOCKED: {t_min:.1f} min — exceeds 25-min threshold; "
            "open caching-strategy-revisit issue"
        )

    mode_str = f"sample (n={sample_n})" if sample_mode else "full"
    py_ver = sys.version.split()[0]

    content = f"""# Timing Spike — {today}

## Machine Info

- Platform: {platform.machine()}
- OS: {platform.system()}
- Python: {py_ver}

## Run Mode

- Mode: {mode_str}
- Addresses: {address_count:,}
- Stops: {stop_count:,}
- Routing results: {result_count:,}

## Stage Timing

| Stage | Time (s) | Time (min) |
|---|---|---|
| network_build | {t_network:.1f} | {t_network / 60:.2f} |
| address_fetch | {t_addresses:.1f} | {t_addresses / 60:.2f} |
| gtfs_fetch | {t_gtfs:.1f} | {t_gtfs / 60:.2f} |
| routing | {t_routing:.1f} | {t_routing / 60:.2f} |
| stratify_lens | {t_lens:.1f} | {t_lens / 60:.2f} |
| stratify_grid | {t_grid:.1f} | {t_grid / 60:.2f} |
| stratify_hex | {t_hex:.1f} | {t_hex / 60:.2f} |
| emit | {t_emit:.1f} | {t_emit / 60:.2f} |
| **Total** | **{t_total:.1f}** | **{t_total / 60:.2f}** |

## Memory

- Peak Python memory (tracemalloc): {peak_mb:.1f} MB
- Note: tracemalloc measures Python allocations only; C extensions (pandana, numpy) \
allocate outside Python's heap.

## Budget Projection

- Total time: {t_min:.2f} min
- Gate threshold: 20 min (within 30-min GHA budget)
- **Verdict: {verdict}**
"""

    _DOCS_DIR.mkdir(parents=True, exist_ok=True)
    doc_path = _DOCS_DIR / f"timing-spike-{today}.md"
    doc_path.write_text(content)
    logger.info("Timing spike document: %s", doc_path)


def _write_lens_verification_doc(
    lens_flags_data: list[dict[str, object]],
) -> None:
    """Write lens verification markdown to pipeline/docs/lens-verification.md."""
    # Derive lens keys from the first row's lens_flags (config-declared order).
    # Emits one column per lens so the doc adapts to any city's lens set.
    lens_keys: list[str] = []
    if lens_flags_data:
        first_flags: dict[str, object] = lens_flags_data[0].get("lens_flags", {})  # type: ignore[assignment]
        lens_keys = list(first_flags.keys())

    header_cells = ["Neighbourhood", *lens_keys, "flag_count"]
    separator_cells = ["---"] * len(header_cells)
    lines = [
        "# Lens Verification — Equity Flag Audit",
        "",
        "| " + " | ".join(header_cells) + " |",
        "| " + " | ".join(separator_cells) + " |",
    ]
    sorted_data = sorted(
        lens_flags_data, key=lambda r: str(r.get("neighborhood_name", ""))
    )
    for row in sorted_data:
        flags: dict[str, object] = row.get("lens_flags", {})  # type: ignore[assignment]
        row_cells = [
            str(row["neighborhood_name"]),
            *[str(flags.get(k, "")) for k in lens_keys],
            str(row.get("lens_flag_count", "")),
        ]
        lines.append("| " + " | ".join(row_cells) + " |")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- EJ Communities filtered to CalEnviroScreen score >= 21 "
            "(top 1/3 of cumulative burden).",
            "- Equity Strategy polygons may not align exactly with Analysis "
            "Neighbourhood boundaries; edge-case addresses can cause a "
            "neighbourhood to inherit an equity flag from an adjacent polygon.",
            "- In sample mode, per-neighbourhood counts are small; a full "
            "run gives more representative flags.",
        ]
    )
    _DOCS_DIR.mkdir(parents=True, exist_ok=True)
    doc_path = _DOCS_DIR / "lens-verification.md"
    doc_path.write_text("\n".join(lines))
    logger.info("Lens verification document: %s", doc_path)
