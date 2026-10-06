import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from gdelt.edges import write_edges
from gdelt.ingest import ingest_range
from gdelt.logger import logger
from gdelt.tracker import IngestTracker


def default_data_dir() -> Path:
    return Path(os.environ.get("GDELT_DATA_DIR", Path.home() / "gdelt-data"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gdelt", description="GDELT event pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    # Options every subcommand shares, added to each one through `parents=`.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-dir", type=Path, default=default_data_dir(),
                        help="where data is stored (default: $GDELT_DATA_DIR or ~/gdelt-data)")

    ingest = commands.add_parser("ingest", parents=[common],
                                 help="download and parse daily GDELT exports")
    when_ingest = ingest.add_mutually_exclusive_group(required=True)
    when_ingest.add_argument("--date", type=date.fromisoformat, help="one day, e.g. 2026-10-03")
    when_ingest.add_argument("--start", type=date.fromisoformat, help="first day of a range (needs --end)")
    when_ingest.add_argument("--yesterday", action="store_true", help="the last finished day in UTC")
    ingest.add_argument("--end", type=date.fromisoformat, help="last day of a range, inclusive")
    ingest.add_argument("--workers", type=int, default=8, help="parallel downloads (default: 8)")

    edges = commands.add_parser(
        "edges", parents=[common],
        help="clean every events file in DATA_DIR/events and build DATA_DIR/edges/edges.parquet",
    )
    edges.add_argument("--max-lag-days", type=int, default=3,
                       help="drop events published more than this many days after "
                            "they happened (default: 3)")
    return parser


def run_ingest(args: argparse.Namespace) -> int:
    if args.yesterday:
        start = end = datetime.now(timezone.utc).date() - timedelta(days=1)
    elif args.date:
        start = end = args.date
    else:
        start, end = args.start, args.end

    tracker = IngestTracker(args.data_dir / "ingest.db")
    try:
        result = ingest_range(start, end, 
                              args.data_dir / "events", 
                              args.workers, tracker=tracker)
    except ValueError as e:   # bad range: start after end, or a day that isn't over yet
        logger.error("%s", e)
        return 2
    finally:
        tracker.close()

    logger.info("Done: %d written, %d empty, %d failed",
                len(result.written), len(result.empty), len(result.failed))
    return 0 if result.ok else 1


def run_edges(args: argparse.Namespace) -> int:
    events_dir = args.data_dir / "events"
    out_path = args.data_dir / "edges" / "edges.parquet"

    try:
        edges = write_edges(events_dir, out_path, args.max_lag_days)
    except FileNotFoundError as e:
        logger.error("%s (run `gdelt ingest` first, or check --data-dir)", e)
        return 1

    countries = pl.concat([edges["source"], edges["target"]]).n_unique()
    logger.info("Wrote %s: %d edges, %d days, %d countries",
                out_path, edges.height, edges["day"].n_unique(), countries)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        if args.start and not args.end:
            parser.error("--start needs --end")
        if args.end and not args.start:
            parser.error("--end can only be used with --start")
        return run_ingest(args)

    if args.command == "edges":
        if args.max_lag_days < 0:
            parser.error("--max-lag-days can't be negative")
        return run_edges(args)

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
