import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from gdelt.ingest import ingest_range
from gdelt.logger import logger
from gdelt.tracker import IngestTracker


def default_data_dir() -> Path:
    return Path(os.environ.get("GDELT_DATA_DIR", Path.home() / "gdelt-data"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gdelt", description="GDELT event pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest", help="download and parse daily GDELT exports")
    when = ingest.add_mutually_exclusive_group(required=True)
    when.add_argument("--date", type=date.fromisoformat, help="one day, e.g. 2026-10-03")
    when.add_argument("--start", type=date.fromisoformat, help="first day of a range (needs --end)")
    when.add_argument("--yesterday", action="store_true", help="the last finished day in UTC")
    ingest.add_argument("--end", type=date.fromisoformat, help="last day of a range, inclusive")
    ingest.add_argument("--data-dir", type=Path, default=default_data_dir(),
                        help="where data is stored (default: $GDELT_DATA_DIR or ~/gdelt-data)")
    ingest.add_argument("--workers", type=int, default=8, help="parallel downloads (default: 8)")
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        if args.start and not args.end:
            parser.error("--start needs --end")
        if args.end and not args.start:
            parser.error("--end can only be used with --start")
        return run_ingest(args)

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
