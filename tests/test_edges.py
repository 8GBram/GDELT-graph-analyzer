from datetime import date, datetime, timezone

import polars as pl
import pytest

from gdelt.clean import clean_events
from gdelt.edges import build_daily_edges
from gdelt.ingest import parse_events
from tests.conftest import event, events

EDGE_COLUMNS = [
    "day", "source", "target", "quad_class",
    "n_events", "goldstein_mean", "tone_mean", "mentions_sum",
]


def edges_of(*rows: dict) -> pl.DataFrame:
    return build_daily_edges(events(*rows))


# --- one row per (day, source, target, quad_class) ---------------------------

def test_single_event_becomes_one_edge():
    out = edges_of(event("1", "DEU", "UKR", quad_class=1,
                         goldstein_scale=1.9, avg_tone=-2.4, num_mentions=2))
    assert out.rows(named=True) == [{
        "day": date(2026, 10, 3), "source": "DEU", "target": "UKR", "quad_class": 1,
        "n_events": 1, "goldstein_mean": 1.9, "tone_mean": -2.4, "mentions_sum": 2,
    }]


def test_aggregates_events_of_the_same_edge():
    out = edges_of(
        event("1", "RUS", "UKR", quad_class=4, goldstein_scale=-10.0, avg_tone=-8.0, num_mentions=10),
        event("2", "RUS", "UKR", quad_class=4, goldstein_scale=-9.0, avg_tone=-6.0, num_mentions=4),
        event("3", "RUS", "UKR", quad_class=4, goldstein_scale=-5.0, avg_tone=-4.0, num_mentions=1),
    )
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["n_events"] == 3
    assert row["goldstein_mean"] == pytest.approx(-8.0)
    assert row["tone_mean"] == pytest.approx(-6.0)
    assert row["mentions_sum"] == 15


def test_quad_classes_are_separate_edges():
    # Cooperation and conflict between the same pair must not be averaged together.
    out = edges_of(
        event("1", "USA", "IRN", quad_class=1, goldstein_scale=3.0),
        event("2", "USA", "IRN", quad_class=4, goldstein_scale=-9.0),
    )
    assert out.select("quad_class", "goldstein_mean").rows() == [(1, 3.0), (4, -9.0)]


def test_edges_are_directed():
    # Russia acting on Ukraine is not the same as Ukraine acting on Russia.
    out = edges_of(event("1", "RUS", "UKR"), event("2", "UKR", "RUS"))
    assert out.select("source", "target").rows() == [("RUS", "UKR"), ("UKR", "RUS")]


def test_different_days_are_separate_edges():
    out = edges_of(
        event("1", "DEU", "UKR", happened=date(2026, 10, 2),
              published=datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc)),
        event("2", "DEU", "UKR", happened=date(2026, 10, 3),
              published=datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc)),
    )
    assert out["day"].to_list() == [date(2026, 10, 2), date(2026, 10, 3)]
    assert out["n_events"].to_list() == [1, 1]


# --- which day an edge belongs to ---------------------------------------------

def test_day_is_the_publish_date_not_the_event_date():
    # Happened Oct 1, published Oct 3: it was only *known* on Oct 3.
    out = edges_of(event("1", "DEU", "UKR", happened=date(2026, 10, 1),
                         published=datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc)))
    assert out["day"].to_list() == [date(2026, 10, 3)]


def test_day_boundary_is_midnight_utc():
    out = edges_of(
        event("1", "DEU", "UKR", published=datetime(2026, 10, 3, 23, 59, tzinfo=timezone.utc)),
        event("2", "DEU", "UKR", happened=date(2026, 10, 4),
              published=datetime(2026, 10, 4, 0, 0, tzinfo=timezone.utc)),
    )
    assert out["day"].to_list() == [date(2026, 10, 3), date(2026, 10, 4)]


# --- shape of the output ------------------------------------------------------

def test_columns_and_types():
    out = edges_of(event("1", "DEU", "UKR"))
    assert out.columns == EDGE_COLUMNS
    assert out.schema["day"] == pl.Date
    assert out.schema["source"] == pl.String
    assert out.schema["target"] == pl.String
    assert out.schema["quad_class"].is_integer()
    assert out.schema["n_events"].is_integer()
    assert out.schema["goldstein_mean"] == pl.Float64
    assert out.schema["tone_mean"] == pl.Float64
    assert out.schema["mentions_sum"].is_integer()


def test_sorted_by_day_source_target_quad():
    out = edges_of(
        event("1", "USA", "CHN", quad_class=3),
        event("2", "DEU", "UKR", quad_class=4,
              published=datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc),
              happened=date(2026, 10, 2)),
        event("3", "USA", "CHN", quad_class=1),
        event("4", "DEU", "UKR", quad_class=1),
    )
    assert out.select("day", "source", "target", "quad_class").rows() == [
        (date(2026, 10, 2), "DEU", "UKR", 4),
        (date(2026, 10, 3), "DEU", "UKR", 1),
        (date(2026, 10, 3), "USA", "CHN", 1),
        (date(2026, 10, 3), "USA", "CHN", 3),
    ]


def test_empty_input_gives_empty_output():
    out = build_daily_edges(events())
    assert out.height == 0
    assert out.columns == EDGE_COLUMNS


def test_accepts_a_lazyframe_and_returns_one():
    df = events(event("1", "DEU", "UKR"), event("2", "DEU", "UKR"), event("3", "UKR", "DEU"))
    out = build_daily_edges(df.lazy())
    assert isinstance(out, pl.LazyFrame)
    assert out.collect().equals(build_daily_edges(df))


# --- end to end on real rows --------------------------------------------------

def test_pipeline_on_real_sample(sample_bytes):
    # The 10-row sample: after cleaning, only the five ARG -> ISR events remain
    # (AFR -> CHN is dropped as a region). All five are verbal cooperation.
    edges = build_daily_edges(clean_events(parse_events(sample_bytes)))
    assert edges.select("day", "source", "target", "quad_class", "n_events").rows() == [
        (date(2026, 10, 4), "ARG", "ISR", 1, 5),
    ]
