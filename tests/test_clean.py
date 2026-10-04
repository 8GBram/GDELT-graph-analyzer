from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from gdelt.clean import clean_events

# Same columns and types that parse_events produces.
EVENT_SCHEMA = {
    "id": pl.String,
    "date": pl.Date,
    "date_added": pl.Datetime("us", "UTC"),
    "actor1_country_code": pl.String,
    "actor2_country_code": pl.String,
    "event_code": pl.String,
    "event_base_code": pl.String,
    "event_root_code": pl.String,
    "quad_class": pl.Int32,
    "goldstein_scale": pl.Float64,
    "num_mentions": pl.Int32,
    "num_sources": pl.Int32,
    "num_articles": pl.Int32,
    "avg_tone": pl.Float64,
}

PUBLISHED = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def event(id: str, a1: str | None, a2: str | None, *,
          happened: date = date(2026, 10, 3),
          published: datetime = PUBLISHED) -> dict:
    """One event row. By default: happened and published on the same day."""
    return {
        "id": id, "date": happened, "date_added": published,
        "actor1_country_code": a1, "actor2_country_code": a2,
        "event_code": "042", "event_base_code": "042", "event_root_code": "04",
        "quad_class": 1, "goldstein_scale": 1.9,
        "num_mentions": 2, "num_sources": 1, "num_articles": 2,
        "avg_tone": -2.4,
    }


def events(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=EVENT_SCHEMA)


def kept_ids(df: pl.DataFrame, **kwargs) -> list[str]:
    return sorted(clean_events(df, **kwargs)["id"].to_list())


# --- countries --------------------------------------------------------------

def test_keeps_country_to_country_event():
    assert kept_ids(events(event("1", "DEU", "UKR"))) == ["1"]


@pytest.mark.parametrize("region", ["EUR", "AFR", "WST", "MEA", "NMR"])
def test_drops_region_codes_on_either_side(region):
    df = events(
        event("1", region, "RUS"),
        event("2", "RUS", region),
    )
    assert kept_ids(df) == []


def test_drops_unknown_codes():
    assert kept_ids(events(event("1", "XYZ", "USA"))) == []


def test_drops_missing_codes():
    df = events(event("1", None, "USA"), event("2", "USA", None))
    assert kept_ids(df) == []


def test_renames_old_cameo_codes_to_iso():
    # CAMEO uses TMP for East Timor; ISO 3166 uses TLS.
    df = events(event("1", "TMP", "AUS"), event("2", "IDN", "TMP"))
    out = clean_events(df).sort("id")
    assert out["actor1_country_code"].to_list() == ["TLS", "IDN"]
    assert out["actor2_country_code"].to_list() == ["AUS", "TLS"]


# --- self-loops -------------------------------------------------------------

def test_drops_self_loops():
    df = events(event("1", "USA", "USA"), event("2", "USA", "CAN"))
    assert kept_ids(df) == ["2"]


def test_self_loop_check_happens_after_renaming():
    # TMP -> TLS is the same country, so this is a self-loop too.
    assert kept_ids(events(event("1", "TMP", "TLS"))) == []


# --- lag --------------------------------------------------------------------

@pytest.mark.parametrize(("days_before", "kept"), [
    (0, True),
    (1, True),
    (3, True),    # the cutoff is inclusive
    (4, False),
    (365, False),  # "a year ago today..." anniversary articles
])
def test_lag_cutoff_default_three_days(days_before, kept):
    happened = date(2026, 10, 3) - timedelta(days=days_before)
    df = events(event("1", "DEU", "UKR", happened=happened))
    assert kept_ids(df) == (["1"] if kept else [])


def test_drops_events_dated_after_publication():
    df = events(event("1", "DEU", "UKR", happened=date(2026, 10, 4)))
    assert kept_ids(df) == []


def test_max_lag_days_is_configurable():
    df = events(
        event("1", "DEU", "UKR", happened=date(2026, 10, 3)),
        event("2", "DEU", "UKR", happened=date(2026, 10, 2)),
        event("3", "DEU", "UKR", happened=date(2026, 9, 26)),
    )
    assert kept_ids(df, max_lag_days=0) == ["1"]
    assert kept_ids(df, max_lag_days=7) == ["1", "2", "3"]


def test_lag_uses_the_utc_publish_date():
    # Published 00:30 UTC on Oct 4 about an Oct 3 event: lag is 1 day, not 0.
    df = events(event("1", "DEU", "UKR",
                      happened=date(2026, 10, 3),
                      published=datetime(2026, 10, 4, 0, 30, tzinfo=timezone.utc)))
    assert kept_ids(df, max_lag_days=0) == []
    assert kept_ids(df, max_lag_days=1) == ["1"]


# --- shape of the output ----------------------------------------------------

def test_keeps_columns_and_types():
    out = clean_events(events(event("1", "DEU", "UKR")))
    assert out.schema == pl.Schema(EVENT_SCHEMA)


def test_empty_input_gives_empty_output():
    out = clean_events(events())
    assert out.height == 0
    assert out.schema == pl.Schema(EVENT_SCHEMA)


def test_all_rules_together():
    df = events(
        event("keep", "DEU", "UKR"),
        event("region", "EUR", "RUS"),
        event("self", "USA", "USA"),
        event("old", "DEU", "UKR", happened=date(2016, 10, 5)),
        event("timor", "TMP", "AUS"),
    )
    assert kept_ids(df) == ["keep", "timor"]
