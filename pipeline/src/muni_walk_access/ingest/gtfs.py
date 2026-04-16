"""GTFS feed fetcher and stop-frequency parser with calendar filtering.

Supports both legacy single-window (v1) and multi-window per-route (v2) modes.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import zipfile

import httpx
import polars as pl

from muni_walk_access.config import Config, TimeWindow
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


def _get_service_ids(zf: zipfile.ZipFile, service_days: str) -> set[str]:
    """Read calendar.txt and return service_ids matching the requested day type.

    Args:
        zf: Open GTFS zip file.
        service_days: One of "weekday", "saturday", or "sunday".

    """
    day_columns = {
        "weekday": ["monday", "tuesday", "wednesday", "thursday", "friday"],
        "saturday": ["saturday"],
        "sunday": ["sunday"],
    }
    cols = day_columns.get(service_days)
    if cols is None:
        raise ValueError(f"Invalid service_days: {service_days!r}")

    try:
        cal_df = pl.read_csv(io.BytesIO(zf.read("calendar.txt")), infer_schema_length=0)
    except KeyError:
        logger.warning("No calendar.txt in GTFS zip; skipping service-day filter")
        return set()

    # Filter to rows where ALL requested day columns are "1"
    mask = pl.lit(True)
    for col in cols:
        mask = mask & (pl.col(col) == "1")
    matching = cal_df.filter(mask)

    service_ids = set(matching["service_id"].to_list())
    logger.info(
        "Calendar filter: service_days=%s → %d service ID(s): %s",
        service_days,
        len(service_ids),
        sorted(service_ids),
    )
    return service_ids


def _compute_stop_frequencies(
    zip_bytes: bytes,
    peak_start_sec: int,
    peak_end_sec: int,
    service_days: str = "weekday",
) -> pl.DataFrame:
    """Parse trips.txt + stop_times.txt + stops.txt from GTFS zip.

    Filters trips to the requested service day type (weekday/saturday/sunday)
    using calendar.txt before computing frequencies.

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

        # Filter trips to matching service day type
        service_ids = _get_service_ids(zf, service_days)

    if service_ids:
        pre_filter = len(trips_df)
        trips_df = trips_df.filter(pl.col("service_id").is_in(list(service_ids)))
        logger.info(
            "Service-day filter: %d → %d trips (%d excluded)",
            pre_filter,
            len(trips_df),
            pre_filter - len(trips_df),
        )

    trip_ids = set(trips_df["trip_id"].to_list())

    # Filter stop_times to valid trips and peak window
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


def _build_time_window_ranges(
    time_windows: list[TimeWindow],
) -> list[tuple[str, int, int]]:
    """Convert TimeWindow configs into (key, start_sec, end_sec) tuples.

    All times are normalised to 0–86399 (mod 86400) so GTFS >24h
    departure times bin correctly.
    """
    return [(tw.key, tw.start_seconds, tw.end_seconds) for tw in time_windows]


def _bin_departure(dep_sec: int, windows: list[tuple[str, int, int]]) -> str | None:
    """Assign a departure second to a time window key, or None if no match.

    Handles >24h GTFS times via mod 86400 and overnight windows where
    end < start (e.g. 19:00–00:00).
    """
    t = dep_sec % 86400
    for key, start, end in windows:
        if end > start:
            # Normal window (e.g. 06:00–09:00)
            if start <= t < end:
                return key
        else:
            # Overnight wrap (e.g. 19:00–00:00 means 19:00–24:00)
            if t >= start or t < end:
                return key
    return None


def _compute_stop_frequencies_v2(
    zip_bytes: bytes,
    time_windows: list[TimeWindow],
    service_days: str = "weekday",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse GTFS zip into per-route detail and per-stop summary DataFrames.

    Returns:
        detail: stop_id, route_id, route_short_name, time_window,
                trips_per_hour, stop_lat, stop_lon
        summary: stop_id, time_window, best_route_headway_min,
                 total_trips_per_hour, route_count, stop_lat, stop_lon

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
            routes_df = pl.read_csv(
                io.BytesIO(zf.read("routes.txt")), infer_schema_length=0
            )
        except KeyError as exc:
            raise IngestError(
                GTFS_DATASET_ID,
                f"GTFS zip missing required file: {exc}",
            ) from exc

        service_ids = _get_service_ids(zf, service_days)

    # Filter trips to matching service day type
    if service_ids:
        pre_filter = len(trips_df)
        trips_df = trips_df.filter(pl.col("service_id").is_in(list(service_ids)))
        logger.info(
            "Service-day filter: %d → %d trips (%d excluded)",
            pre_filter,
            len(trips_df),
            pre_filter - len(trips_df),
        )

    # Join route_id onto stop_times via trips
    # trips_df has: trip_id, route_id, service_id, ...
    # stop_times_df has: trip_id, stop_id, departure_time, ...
    trip_routes = trips_df.select(["trip_id", "route_id"])
    stop_times_df = stop_times_df.join(trip_routes, on="trip_id", how="inner")

    # Parse departure_time to seconds
    stop_times_df = stop_times_df.with_columns(
        pl.col("departure_time")
        .map_elements(_parse_time_seconds, return_dtype=pl.Int64)
        .alias("dep_sec")
    ).filter(pl.col("dep_sec").is_not_null())

    # Bin each departure into a time window
    tw_ranges = _build_time_window_ranges(time_windows)
    stop_times_df = stop_times_df.with_columns(
        pl.col("dep_sec")
        .map_elements(lambda s: _bin_departure(s, tw_ranges), return_dtype=pl.Utf8)
        .alias("time_window")
    ).filter(pl.col("time_window").is_not_null())

    # Build duration lookup for trips_per_hour calculation
    tw_hours = {tw.key: tw.duration_hours for tw in time_windows}

    # Join route_short_name from routes.txt
    route_names = routes_df.select(
        [
            pl.col("route_id"),
            pl.col("route_short_name").alias("route_short_name"),
        ]
    )
    stop_times_df = stop_times_df.join(route_names, on="route_id", how="left")
    # Some GTFS feeds have null route_short_name; fall back to route_id
    stop_times_df = stop_times_df.with_columns(
        pl.col("route_short_name").fill_null(pl.col("route_id"))
    )

    # Stop coordinates
    stops_coords = stops_df.select(
        [
            pl.col("stop_id").cast(pl.Utf8),
            pl.col("stop_lat").cast(pl.Float64),
            pl.col("stop_lon").cast(pl.Float64),
        ]
    )

    # --- Per-route detail ---
    # Group by (stop_id, route_id, time_window) → count distinct trips
    detail = stop_times_df.group_by(
        ["stop_id", "route_id", "route_short_name", "time_window"]
    ).agg(pl.col("trip_id").n_unique().alias("trip_count"))

    # Add trips_per_hour using the window duration
    detail = detail.with_columns(
        pl.struct(["trip_count", "time_window"])
        .map_elements(
            lambda row: round(
                row["trip_count"] / tw_hours.get(row["time_window"], 1.0), 2
            ),
            return_dtype=pl.Float64,
        )
        .alias("trips_per_hour")
    )

    # Cast stop_id for join consistency
    detail = detail.with_columns(pl.col("stop_id").cast(pl.Utf8))
    detail = detail.join(stops_coords, on="stop_id", how="left")

    # Drop stops with null coordinates
    detail = detail.filter(
        pl.col("stop_lat").is_not_null() & pl.col("stop_lon").is_not_null()
    )

    detail = detail.select(
        [
            "stop_id",
            "route_id",
            "route_short_name",
            "time_window",
            "trips_per_hour",
            "stop_lat",
            "stop_lon",
        ]
    )

    # --- Summary per (stop_id, time_window) ---
    summary = (
        detail.group_by(["stop_id", "time_window"])
        .agg(
            pl.col("trips_per_hour").sum().alias("total_trips_per_hour"),
            pl.col("trips_per_hour").max().alias("_max_route_tph"),
            pl.col("route_id").n_unique().alias("route_count"),
            pl.first("stop_lat"),
            pl.first("stop_lon"),
        )
        .with_columns(
            # best_route_headway_min = 60 / max single-route trips_per_hour
            # Clamp inf→null for stops where _max_route_tph is 0
            pl.when(pl.col("_max_route_tph") > 0)
            .then((60.0 / pl.col("_max_route_tph")).round(1))
            .otherwise(None)
            .alias("best_route_headway_min")
        )
        .drop("_max_route_tph")
    )

    logger.info(
        "Frequencies v2: %d detail rows, %d summary rows across %d windows",
        len(detail),
        len(summary),
        len(time_windows),
    )

    # Log per-window stats
    for tw in time_windows:
        tw_summary = summary.filter(pl.col("time_window") == tw.key)
        if len(tw_summary) > 0:
            median_headway = tw_summary["best_route_headway_min"].median()
            logger.info(
                "  %s: %d stops, median headway %.1f min",
                tw.key,
                len(tw_summary),
                median_headway or 0.0,
            )

    return detail, summary


def fetch_gtfs(
    config: Config,
    client: httpx.Client | None = None,
) -> tuple[pl.DataFrame, str, str]:
    """Download GTFS zip, parse AM-peak stop frequencies.

    Returns (df, sha256, feed_date).

    Falls back to cache if all upstream URLs fail. Raises IngestError if
    no upstream and no cache exist.

    Returns:
        df: DataFrame with columns [stop_id, trips_per_hour_peak, stop_lat, stop_lon]
        sha256: hex digest of the raw zip bytes
        feed_date: Last-Modified date from GTFS server, or empty string

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

        # Read feed date from saved HTTP metadata
        feed_date = ""
        if meta_path.exists():
            try:
                _meta = json.loads(meta_path.read_text())
                feed_date = _meta.get("last_modified", "")
            except (json.JSONDecodeError, OSError):
                pass

        # Cache key includes service_days so weekday/weekend don't collide
        service_days = config.frequency.service_days
        parsed_cache_id = f"{GTFS_DATASET_ID}-{service_days}-{sha256[:16]}"
        fresh_parquet = cache.get(CACHE_SUBDIR, parsed_cache_id)
        if fresh_parquet is not None:
            df = pl.read_parquet(fresh_parquet)
            return df, sha256, feed_date

        # Cache the raw zip (only if we got new data from upstream)
        if not use_cache:
            cache.put(CACHE_SUBDIR, GTFS_DATASET_ID + "-zip", zip_bytes, "zip")

        # Parse the zip
        df = _compute_stop_frequencies(
            zip_bytes, peak_start, peak_end, service_days=service_days
        )

        # Cache the parsed result keyed by content hash
        buf = io.BytesIO()
        df.write_parquet(buf)
        cache.put(CACHE_SUBDIR, parsed_cache_id, buf.getvalue(), "parquet")

        return df, sha256, feed_date

    finally:
        if own_client:
            _client.close()


def fetch_gtfs_v2(
    config: Config,
    client: httpx.Client | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, str, str]:
    """Download GTFS zip, parse per-route multi-window frequencies.

    Falls back to cache if all upstream URLs fail. Raises IngestError if
    no upstream and no cache exist.

    Returns:
        detail: Per-route detail DataFrame
        summary: Per-stop summary DataFrame (drives hex scoring)
        sha256: hex digest of the raw zip bytes
        feed_date: Last-Modified date from GTFS server, or empty string

    """
    time_windows = config.frequency.time_windows
    if not time_windows:
        raise ValueError(
            "fetch_gtfs_v2 requires config.frequency.time_windows to be set"
        )

    cache = CacheManager(
        root=config.ingest.cache_dir,
        ttl_days=config.ingest.cache_ttl_days,
    )

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

        # Read feed date from saved HTTP metadata
        feed_date = ""
        if meta_path.exists():
            try:
                _meta = json.loads(meta_path.read_text())
                feed_date = _meta.get("last_modified", "")
            except (json.JSONDecodeError, OSError):
                pass

        # Cache key differentiates v2 from v1
        service_days = config.frequency.service_days
        tw_keys = "-".join(tw.key for tw in time_windows)
        parsed_cache_id = f"{GTFS_DATASET_ID}-v2-{service_days}-{tw_keys}-{sha256[:16]}"

        # Check for cached detail+summary parquets
        detail_cached = cache.get(CACHE_SUBDIR, f"{parsed_cache_id}-detail")
        summary_cached = cache.get(CACHE_SUBDIR, f"{parsed_cache_id}-summary")
        if detail_cached is not None and summary_cached is not None:
            detail = pl.read_parquet(detail_cached)
            summary = pl.read_parquet(summary_cached)
            return detail, summary, sha256, feed_date

        # Cache the raw zip (only if we got new data from upstream)
        if not use_cache:
            cache.put(CACHE_SUBDIR, GTFS_DATASET_ID + "-zip", zip_bytes, "zip")

        # Parse
        detail, summary = _compute_stop_frequencies_v2(
            zip_bytes, time_windows, service_days=service_days
        )

        # Cache both DataFrames
        buf_d = io.BytesIO()
        detail.write_parquet(buf_d)
        cache.put(
            CACHE_SUBDIR, f"{parsed_cache_id}-detail", buf_d.getvalue(), "parquet"
        )

        buf_s = io.BytesIO()
        summary.write_parquet(buf_s)
        cache.put(
            CACHE_SUBDIR, f"{parsed_cache_id}-summary", buf_s.getvalue(), "parquet"
        )

        return detail, summary, sha256, feed_date

    finally:
        if own_client:
            _client.close()
