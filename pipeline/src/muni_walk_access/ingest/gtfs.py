"""GTFS feed fetcher and AM-peak stop-frequency parser."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import zipfile

import httpx
import polars as pl

from muni_walk_access.config import Config
from muni_walk_access.exceptions import IngestError
from muni_walk_access.ingest.cache import CacheManager
from muni_walk_access.ingest.datasf import set_upstream_fallback

logger = logging.getLogger(__name__)

# Canonical download page: https://www.sfmta.com/reports/gtfs-transit-data
# Old DataSF dataset 2qyp-77cq is deprecated. Old gtfs.sfmta.com URL dead.
GTFS_URL = "https://muni-gtfs.apps.sfmta.com/data/muni_gtfs-current.zip"
GTFS_DATASET_ID = "muni-gtfs"
CACHE_SUBDIR = "gtfs"
_META_FILENAME = f"{GTFS_DATASET_ID}-http.json"


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
    """Parse trips.txt + stop_times.txt + stops.txt from GTFS zip.

    Returns DataFrame with columns:
        stop_id (str), trips_per_hour_peak (float), stop_lat (float), stop_lon (float).
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
            trips_df = pl.read_csv(
                io.BytesIO(zf.read("trips.txt")), infer_schema_length=0
            )
            stop_times_df = pl.read_csv(
                io.BytesIO(zf.read("stop_times.txt")), infer_schema_length=0
            )
            stops_df = pl.read_csv(
                io.BytesIO(zf.read("stops.txt")), infer_schema_length=0
            )
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

    # Join stop coordinates from stops.txt
    stops_coords = stops_df.select(
        [
            pl.col("stop_id").cast(pl.Utf8),
            pl.col("stop_lat").cast(pl.Float64),
            pl.col("stop_lon").cast(pl.Float64),
        ]
    )
    stop_trip_counts = stop_trip_counts.join(stops_coords, on="stop_id", how="left")

    # Drop stops with null coordinates (stop_id in stop_times but not in stops.txt).
    pre_join = len(stop_trip_counts)
    stop_trip_counts = stop_trip_counts.filter(
        pl.col("stop_lat").is_not_null() & pl.col("stop_lon").is_not_null()
    )
    if len(stop_trip_counts) < pre_join:
        logger.warning(
            "Dropped %d stop(s) with no coordinates in stops.txt",
            pre_join - len(stop_trip_counts),
        )

    return stop_trip_counts


def fetch_gtfs(
    config: Config,
    client: httpx.Client | None = None,
) -> tuple[pl.DataFrame, str]:
    """Download GTFS zip, parse AM-peak stop frequencies, return (df, sha256).

    Falls back to cache if all upstream URLs fail. Raises IngestError if
    no upstream and no cache exist.

    Returns:
        df: DataFrame with columns [stop_id, trips_per_hour_peak, stop_lat, stop_lon]
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

    # Load saved HTTP metadata for conditional requests
    meta_path = cache._dir(CACHE_SUBDIR) / _META_FILENAME
    http_meta: dict[str, str] = {}
    if meta_path.exists():
        try:
            http_meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    try:
        # Conditional fetch — skip download if server content unchanged
        headers: dict[str, str] = {}
        if http_meta.get("etag"):
            headers["If-None-Match"] = http_meta["etag"]
        if http_meta.get("last_modified"):
            headers["If-Modified-Since"] = http_meta["last_modified"]

        use_cache = False
        try:
            resp = _client.get(GTFS_URL, headers=headers)
            if resp.status_code == 304:
                logger.info("GTFS unchanged (304 Not Modified), using cache")
                use_cache = True
            else:
                resp.raise_for_status()
                zip_bytes = resp.content
                # Save HTTP metadata for next conditional request
                new_meta: dict[str, str] = {}
                if resp.headers.get("etag"):
                    new_meta["etag"] = resp.headers["etag"]
                if resp.headers.get("last-modified"):
                    new_meta["last_modified"] = resp.headers["last-modified"]
                if new_meta:
                    meta_path.write_text(json.dumps(new_meta))
        except (httpx.HTTPError, httpx.TransportError) as exc:
            logger.warning("GTFS fetch failed from %s: %s", GTFS_URL, exc)
            use_cache = True

        if use_cache or zip_bytes is None:
            cached_zip = cache.get_any(CACHE_SUBDIR, GTFS_DATASET_ID + "-zip")
            if cached_zip is not None and cached_zip.suffix == ".zip":
                if not use_cache:
                    logger.warning("GTFS fetch failed; using cached zip %s", cached_zip)
                    set_upstream_fallback()
                zip_bytes = cached_zip.read_bytes()
            else:
                raise IngestError(
                    GTFS_DATASET_ID,
                    "GTFS fetch failed and no local cache. "
                    "Warm the cache with network access first.",
                )

        sha256 = hashlib.sha256(zip_bytes).hexdigest()

        # Check if we have a parsed Parquet cached for this exact zip
        parsed_cache_id = f"{GTFS_DATASET_ID}-{sha256[:16]}"
        fresh_parquet = cache.get(CACHE_SUBDIR, parsed_cache_id)
        if fresh_parquet is not None:
            df = pl.read_parquet(fresh_parquet)
            return df, sha256

        # Cache the raw zip (only if we got new data from upstream)
        if not use_cache:
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
