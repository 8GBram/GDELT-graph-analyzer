from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from io import BytesIO
from zipfile import ZipFile

import polars as pl
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 

from gdelt.logger import logger
from gdelt.config import GDELT_SCHEMA
from gdelt.tracker import IngestTracker

def _make_session(pool_size: int = 16) -> requests.session:
    """
    A session that reuses connections and retries temporary server errors.
    """
    
    retry = Retry(
        total = 5,
        backoff_factor = 1, #waits ~1s, 2s, 4s, 8s, 16s
        status_forcelist = [429, 500, 502, 503, 504], # missing files not retried
        allowed_methods = ["GET"],
        raise_on_status = False, #after last retry, return response
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=pool_size)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

_session = _make_session()
    
def download_export(timestamp: str) -> bytes | None:
    """
    Returns unzipped CSV bytes of exports.CSV.zip for that timestamp
    
    Input:
        timestamp: str in this format (yyyymmddhhmmss in UTC) 
            Example: "20261004080000"
    """
    
    url = f"https://data.gdeltproject.org/gdeltv2/{timestamp}.export.CSV.zip"
    
    response = _session.get(url, timeout=30)
    if response.status_code == 404:
        logger.warning("Export %s not available (404)", timestamp)
        return None
    
    if response.status_code != 200:
        response.raise_for_status()
            
    with ZipFile(BytesIO(response.content)) as z:
        csv_filename = z.namelist()[0]

        # Read the raw uncompressed bytes
        csv_bytes = z.read(csv_filename)
    
    return csv_bytes
        
def parse_events(csv_bytes: bytes) -> pl.DataFrame:
    """
    Parses 
        columns id, date, actor country codes, event codes,
        goldstein scale, and other metrics into a polars Dataframe
    """
    
    df = pl.scan_csv(
        csv_bytes,
        separator="\t",
        has_header=False,
        schema=GDELT_SCHEMA,
        quote_char=None
    )
    
    df = df.with_columns(
        pl.col("raw_date").str.to_date(format="%Y%m%d").alias("date"),
        pl.col("date_added")
        .cast(pl.String)
        .str.to_datetime(format="%Y%m%d%H%M%S", time_zone="UTC"),
    )

    df = df.select(
        ["id", "date", "date_added",
         "actor1_country_code", "actor2_country_code",
         "event_code", "event_base_code", "event_root_code",
         "quad_class", "goldstein_scale",
         "num_mentions", "num_sources", "num_articles",
         "avg_tone"
        ]
    )
    
    df = df.filter(
        pl.col("actor1_country_code").is_not_null() 
        & pl.col("actor2_country_code").is_not_null()
    )

    return df.collect()

def day_timestamps(day: date) -> list[str]:
    """
    The 96 GDELT timestamps for one day: 
    '20261004000000', '20261004001500', ... '20261004234500'.
    """
    prefix = day.strftime("%Y%m%d")
    
    return [
        f"{prefix}{hour:02d}{minute:02d}00"
        for hour in range(24)
        for minute in range(0, 60, 15)
    ] 
    
def _fetch(timestamp: str) -> tuple[str, bytes | None, Exception | None]:
    """
    Run download_export, and Returns Error incase
    """
    try:
        return timestamp, download_export(timestamp), None
    except requests.RequestException as e:
        return timestamp, None, e


def ingest_day(day: date,
               out_dir: Path,
               max_workers: int = 8,
               tracker: IngestTracker | None = None) -> Path | None:
    """
    Download and parse all exports published on `day`,
    Write then to out_dir/YYYY-MM-DD.parquet

    Skips the download if that file already exists. Returns the path, or None if no data was found.
    If a tracker is given, the outcome of every timestamp is recorded in it.
    """
    path = out_dir / f"{day.isoformat()}.parquet"
    if path.exists():
        logger.info("Skipping %s, already ingested", day)
        return path

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_fetch, day_timestamps(day)))

    # Record every outcome here in the main thread: a SQLite connection
    # can only be used from the thread that created it.
    frames = []
    errors = []
    for timestamp, csv_bytes, error in results:
        if error is not None:
            status, rows = "failed", None
            errors.append(error)
            logger.warning("Export %s failed: %s", timestamp, error)
        elif csv_bytes is None:
            status, rows = "missing", None
        else:
            frame = parse_events(csv_bytes)
            frames.append(frame)
            status, rows = "ok", frame.height

        if tracker is not None:
            tracker.record(timestamp, status, rows)

    # Raise before writing anything, so a failed day leaves no file behind
    # and is retried on the next run.
    if errors:
        raise errors[0]

    if not frames:
        logger.warning("No exports found for %s", day)
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pl.concat(frames).write_parquet(tmp)
    tmp.replace(path)

    logger.info("Wrote %s (%d of %d exports)", path.name, len(frames), len(results))
    return path

def ingest_range(start: date,
                 end: date,
                 out_dir: Path,
                 max_workers: int = 8,
                 tracker: IngestTracker | None = None) -> list[Path]:

    """
    Runs ingest_day for every day from start to end (inclusive)
    Returns the files written or found
    """
    if start > end:
        raise ValueError(f"start ({start}) is after end ({end})")

    today = datetime.now(timezone.utc).date()
    if end >= today:
        raise ValueError(f"end ({end}) must be before today in UTC ({today})")

    paths = []
    day = start
    while day <= end:
        path = ingest_day(day, out_dir, max_workers, tracker=tracker)
        if path is not None:
            paths.append(path)
        day += timedelta(days=1)

    return paths
    