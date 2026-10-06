import polars as pl


def build_daily_edges(events: pl.LazyFrame) -> pl.LazyFrame:
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