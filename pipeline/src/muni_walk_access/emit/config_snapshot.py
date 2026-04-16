"""Config snapshot emitter for the muni-walk-access data contract."""

from __future__ import annotations

import logging
from pathlib import Path

from muni_walk_access.emit.schemas import CodeVersion, ConfigSnapshot, DataVersions

logger = logging.getLogger(__name__)


def write_config_snapshot(
    run_id: str,
    git_sha: str,
    git_tag: str,
    config_hash: str,
    gtfs_sha256: str,
    gtfs_feed_date: str,
    osm_date: str,
    datasf_timestamps: dict[str, str],
    datasf_data_updated: dict[str, str],
    upstream_fallback: bool,
    config_values: dict[str, object],
    output_dir: Path,
) -> Path:
    """Write config_snapshot.json to {output_dir}/site/src/data/.

    Redacts ingest.cache_dir if it is an absolute path to preserve determinism.
    Returns the path to the written file.
    """
    sanitized: dict[str, object] = dict(config_values)
    ingest_cfg = sanitized.get("ingest")
    if isinstance(ingest_cfg, dict):
        cache_dir = ingest_cfg.get("cache_dir")
        if isinstance(cache_dir, str) and Path(cache_dir).is_absolute():
            sanitized["ingest"] = {**ingest_cfg, "cache_dir": "<redacted>"}

    snapshot = ConfigSnapshot(
        run_id=run_id,
        code_version=CodeVersion(git_sha=git_sha, git_tag=git_tag),
        config_hash=config_hash,
        data_versions=DataVersions(
            gtfs_feed_sha256=gtfs_sha256,
            gtfs_feed_date=gtfs_feed_date,
            osm_extract_date=osm_date,
            datasf_timestamps=datasf_timestamps,
            datasf_data_updated=datasf_data_updated,
        ),
        config_values=sanitized,
        upstream_fallback=upstream_fallback,
    )

    out_dir = output_dir / "site" / "src" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "config_snapshot.json"
    out_path.write_text(snapshot.model_dump_json(indent=2))
    logger.info("Config snapshot written: %s", out_path)
    return out_path
