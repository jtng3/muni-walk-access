"""Filesystem cache manager for ingested datasets."""

from __future__ import annotations

from datetime import date
from pathlib import Path


class CacheManager:
    """Manages read/write of cached dataset files under a root cache directory.

    Cache keys follow the pattern: <dataset-id>-<yyyymmdd>.<ext>
    TTL is checked by comparing the file date suffix to today's date,
    then counting days elapsed.
    """

    def __init__(self, root: Path, ttl_days: int = 30) -> None:
        """Initialise with cache root directory and TTL in days."""
        self._root = root
        self._ttl_days = ttl_days

    def _dir(self, subdir: str) -> Path:
        d = self._root / subdir
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _today_str(self) -> str:
        return date.today().strftime("%Y%m%d")

    def _fresh_path(self, subdir: str, dataset_id: str, ext: str) -> Path:
        """Return the expected path for today's cache entry."""
        return self._dir(subdir) / f"{dataset_id}-{self._today_str()}.{ext}"

    def _find_existing(self, subdir: str, dataset_id: str) -> list[Path]:
        """Return all cached files for dataset_id sorted newest-first."""
        d = self._dir(subdir)
        for ext in ("parquet", "geojson", "zip"):
            matches = sorted(d.glob(f"{dataset_id}-*.{ext}"), reverse=True)
            if matches:
                return matches
        return []

    def _is_fresh(self, path: Path) -> bool:
        """Return True if the cached file is within TTL."""
        stem = path.stem  # e.g. "i28k-bkz6-20260412"
        parts = stem.rsplit("-", 1)
        if len(parts) != 2:
            return False
        date_str = parts[1]
        try:
            file_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except (ValueError, IndexError):
            return False
        delta = (date.today() - file_date).days
        return delta < self._ttl_days

    def get(self, subdir: str, dataset_id: str) -> Path | None:
        """Return fresh cache path, or None if stale/missing."""
        candidates = self._find_existing(subdir, dataset_id)
        if not candidates:
            return None
        newest = candidates[0]
        return newest if self._is_fresh(newest) else None

    def get_any(self, subdir: str, dataset_id: str) -> Path | None:
        """Return ANY cached path (even stale) for fallback, or None if missing."""
        candidates = self._find_existing(subdir, dataset_id)
        return candidates[0] if candidates else None

    def put(self, subdir: str, dataset_id: str, data: bytes, ext: str) -> Path:
        """Write raw bytes to cache, return the written path."""
        path = self._fresh_path(subdir, dataset_id, ext)
        path.write_bytes(data)
        return path
