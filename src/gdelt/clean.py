import polars as pl
import pycountry

# Real countries only: ISO 3166 alpha-3 codes. CAMEO also uses region and
# group codes (EUR, AFR, WST, ...) that can't be nodes in a country graph.
ISO_CODES: frozenset[str] = frozenset(c.alpha_3 for c in pycountry.countries)

# Old CAMEO country codes that differ from ISO 3166.
CAMEO_TO_ISO: dict[str, str] = {
    "TMP": "TLS",  # East Timor
}

ACTOR_COLUMNS = ("actor1_country_code", "actor2_country_code")


def clean_events(df: pl.DataFrame,
                  max_lag_days: int = 3) -> pl.DataFrame:
    """
    Keep only events that make sense as edges between two countries:
      - both actors are real countries (old CAMEO codes renamed to ISO first)
      - the two countries differ (domestic events are dropped)
      - the event happened 0 to `max_lag_days` days before GDELT published it,
        which drops retrospective "a year ago today..." articles

    Only rows are removed; columns and types are unchanged.
    """
    actor1, actor2 = (pl.col(name) for name in ACTOR_COLUMNS)
    lag_days = (pl.col("date_added").dt.date() - pl.col("date")).dt.total_days()

    return (
        df
        # Rename before the self-loop check, so TMP -> TLS counts as one country.
        .with_columns(pl.col(name).replace(CAMEO_TO_ISO) for name in ACTOR_COLUMNS)
        .filter(
            actor1.is_in(ISO_CODES),
            actor2.is_in(ISO_CODES),
            actor1 != actor2,
            lag_days.is_between(0, max_lag_days),
        )
    )
