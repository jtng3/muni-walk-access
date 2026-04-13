"""GTFS feed fetcher and AM-peak stop-frequency parser."""

from __future__ import annotations

import hashlib
import io
import logging
import zipfile

import httpx
import polars as pl

from muni_walk_access.config import Config
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import set_upstream_fallback

logger = logging.getLogger(__name__)

GTFS_URLS = [
    "https://data.sfgov.org/api/views/2qyp-77cq/rows.csv?accessType=DOWNLOAD",
    "https://gtfs.sfmta.com/transitdata/google_transit.zip",
]
GTFS_DATASET_ID = "2qyp-77cq"
CACHE_SUBDIR = "gtfs"


def _parse_time_seconds(t: str) -> int | None:
    """Convert HH:MM:SS to total seconds (handles >24h GTFS times).

    Returns None for malformed/empty time strings.
    """
    try:
        parts = t.strip().split(":")
        if len(parts) < 3:
            return None
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        return h * 3600 + m * 60 + s
    except (ValueError, AttributeError):
        return None


def _parse_peak_seconds(time_str: str) -> int:
    """Parse peak window time string 'HH:MM' to seconds-since-midnight."""
    h, m = time_str.split(":")
    return int(h) * 3600 + int(m) * 60


def _compute_stop_frequencies(
    zip_bytes: bytes,
    peak_start_sec: int,
    peak_end_sec: int,
) -> pl.DataFrame:
    """Parse trips.txt + stop_times.txt from GTFS zip, return per-stop trip counts.

    Returns DataFrame with columns: stop_id (str), trips_per_hour_peak (float).
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as exc:
        raise IngestError(
            GTFS_DATASET_ID,
            f"Downloaded content is not a valid zip file: {exc}",
        ) from exc

    with zf:
        try:
            trips_df = pl.read_csv(io.BytesIO(zf.read("trips.txt")))
            stop_times_df = pl.read_csv(io.BytesIO(zf.read("stop_times.txt")))
        except KeyError as exc:
            raise IngestError(
                GTFS_DATASET_ID,
                f"GTFS zip missing required file: {exc}",
            ) from exc

    trip_ids = set(trips_df["trip_id"].to_list())

    # Filter stop_times to valid trips and AM peak window
    stop_times_df = stop_times_df.filter(pl.col("trip_id").is_in(list(trip_ids)))

    # Parse departure_time to seconds (None for malformed times)
    stop_times_df = stop_times_df.with_columns(
        pl.col("departure_time")
        .map_elements(_parse_time_seconds, return_dtype=pl.Int64)
        .alias("dep_sec")
    ).filter(pl.col("dep_sec").is_not_null())

    peak_df = stop_times_df.filter(
        (pl.col("dep_sec") >= peak_start_sec) & (pl.col("dep_sec") < peak_end_sec)
    )

    # Count distinct trips per stop in peak window
    window_hours = (peak_end_sec - peak_start_sec) / 3600.0

    stop_trip_counts = (
        peak_df.group_by("stop_id")
        .agg(pl.col("trip_id").n_unique().alias("trip_count"))
        .with_columns(
            (pl.col("trip_count") / window_hours).alias("trips_per_hour_peak")
        )
        .select(["stop_id", "trips_per_hour_peak"])
    )

    # Cast stop_id to str for consistency
    stop_trip_counts = stop_trip_counts.with_columns(pl.col("stop_id").cast(pl.Utf8))

    return stop_trip_counts


def fetch_gtfs(
    config: Config,
    client: httpx.Client | None = None,
) -> tuple[pl.DataFrame, str]:
    """Download GTFS zip, parse AM-peak stop frequencies, return (df, sha256).

    Falls back to cache if all upstream URLs fail. Raises IngestError if
    no upstream and no cache exist.

    Returns:
        df: DataFrame with columns [stop_id, trips_per_hour_peak]
        sha256: hex digest of the raw zip bytes

    """
    cache = CacheManager(
        root=config.ingest.cache_dir,
        ttl_days=config.ingest.cache_ttl_days,
    )

    peak_start = _parse_peak_seconds(config.frequency.peak_am_start)
    peak_end = _parse_peak_seconds(config.frequency.peak_am_end)

    own_client = client is None
    _client: httpx.Client = (
        client
        if client is not None
        else httpx.Client(timeout=120.0, follow_redirects=True)
    )

    zip_bytes: bytes | None = None
    sha256: str = ""

    try:
        # Try each upstream URL in order
        last_exc: Exception | None = None
        for url in GTFS_URLS:
            try:
                resp = _client.get(url)
                resp.raise_for_status()
                zip_bytes = resp.content
                break
            except (httpx.HTTPError, httpx.TransportError) as exc:
                logger.warning("GTFS fetch failed from %s: %s", url, exc)
                last_exc = exc

        if zip_bytes is None:
            # All URLs failed — try cache
            stale_zip = cache.get_any(CACHE_SUBDIR, GTFS_DATASET_ID + "-zip")
            if stale_zip is not None and stale_zip.suffix == ".zip":
                logger.warning("All GTFS URLs failed; using cached zip %s", stale_zip)
                set_upstream_fallback()
                zip_bytes = stale_zip.read_bytes()
            else:
                raise IngestError(
                    GTFS_DATASET_ID,
                    f"All GTFS URLs failed and no local cache: {last_exc}. "
                    "Warm the cache with network access first.",
                ) from last_exc

        sha256 = hashlib.sha256(zip_bytes).hexdigest()

        # Check if we have a parsed Parquet cached for this exact zip
        parsed_cache_id = f"{GTFS_DATASET_ID}-{sha256[:16]}"
        fresh_parquet = cache.get(CACHE_SUBDIR, parsed_cache_id)
        if fresh_parquet is not None:
            df = pl.read_parquet(fresh_parquet)
            return df, sha256

        # Cache the raw zip
        cache.put(CACHE_SUBDIR, GTFS_DATASET_ID + "-zip", zip_bytes, "zip")

        # Parse the zip
        df = _compute_stop_frequencies(zip_bytes, peak_start, peak_end)

        # Cache the parsed result keyed by content hash
        buf = io.BytesIO()
        df.write_parquet(buf)
        cache.put(CACHE_SUBDIR, parsed_cache_id, buf.getvalue(), "parquet")

        return df, sha256

    finally:
        if own_client:
            _client.close()
