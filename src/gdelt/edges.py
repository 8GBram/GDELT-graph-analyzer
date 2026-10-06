from pathlib import Path

import polars as pl

from gdelt.clean import PLFrame, clean_events


def build_daily_edges(events: PLFrame) -> PLFrame:
    """
    Aggregate cleaned events into one directed edge per
    (day, source country, target country, quad_class), with:
      - n_events:       how many events the edge combines
      - goldstein_mean: average Goldstein score (-10 conflict to +10 cooperation)
      - tone_mean:      average tone of the source articles
      - mentions_sum:   total mentions, a measure of media attention

    `day` is the UTC date the events were *published* (date_added), not the
    date they happened (date). A day's graph then holds only what was known
    by the end of that day, so a model trained on it can't see reports that
    were published later.

    Expects events that already went through clean_events. Output is sorted
    by day, source, target, quad_class.
    """
    edges = (
        events
        .with_columns(
            pl.col("date_added").dt.date().alias("day")
        )
        .group_by(["day", "actor1_country_code", "actor2_country_code", "quad_class"])
        .agg(
            n_events=pl.len(),
            goldstein_mean=pl.col("goldstein_scale").mean(),
            tone_mean=pl.col("avg_tone").mean(),
            mentions_sum=pl.col("num_mentions").sum()
        )
        .rename({
            "actor1_country_code": "source",
            "actor2_country_code": "target"
        })
        .sort(["day", "source", "target", "quad_class"])
    )
    
    return edges


def write_edges(events_dir: Path, out_path: Path, max_lag_days: int = 3) -> pl.DataFrame:
    """
    Rebuild the edges file from every daily events file in `events_dir`:
    clean_events, then build_daily_edges, written to `out_path`.

    Always rebuilds from scratch: it takes about a second for 90 days, so there's
    nothing to track, and changing a cleaning setting just means running it again.
    Returns the edges that were written.

    Raises FileNotFoundError if `events_dir` has no .parquet files.
    """
    files = sorted(events_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no events files (*.parquet) in {events_dir}")

    edges = build_daily_edges(clean_events(pl.scan_parquet(files), max_lag_days)).collect()

    # Write to a temp file, then rename: a crash mid-write can't leave a
    # half-written edges file behind.
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.tmp")
    edges.write_parquet(tmp)
    tmp.replace(out_path)

    return edges
