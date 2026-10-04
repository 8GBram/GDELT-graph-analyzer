from datetime import date, datetime, timezone

import polars as pl

from gdelt.ingest import parse_events
from tests.conftest import make_row

EXPECTED_COLUMNS = [
    "id", "date", "date_added",
    "actor1_country_code", "actor2_country_code",
    "event_code", "event_base_code", "event_root_code",
    "quad_class", "goldstein_scale",
    "num_mentions", "num_sources", "num_articles",
    "avg_tone",
]


def test_returns_expected_columns(sample_bytes):
    df = parse_events(sample_bytes)
    assert df.columns == EXPECTED_COLUMNS


def test_keeps_only_rows_with_both_countries(sample_bytes):
    df = parse_events(sample_bytes)
    assert df.height == 6
    assert df["actor1_country_code"].null_count() == 0
    assert df["actor2_country_code"].null_count() == 0


def test_drops_merz_row_with_missing_actor1(sample_bytes):
    # Event 1326216042: "Merz arrives in Kyiv" — GDELT failed to code Actor1.
    df = parse_events(sample_bytes)
    assert "1326216042" not in df["id"].to_list()


def test_column_types(sample_bytes):
    schema = parse_events(sample_bytes).schema
    assert schema["date"] == pl.Date
    assert schema["date_added"] == pl.Datetime(time_zone="UTC")
    assert schema["event_code"] == pl.String
    assert schema["goldstein_scale"] == pl.Float64
    assert schema["avg_tone"] == pl.Float64
    assert schema["num_mentions"].is_integer()


def test_parses_date(sample_bytes):
    df = parse_events(sample_bytes)
    assert df["date"][0] == date(2026, 10, 4)


def test_parses_date_added_as_utc(sample_bytes):
    df = parse_events(sample_bytes)
    assert df["date_added"][0] == datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)


def test_keeps_leading_zeros_in_event_codes():
    row = make_row(
        id="1", raw_date="20261004",
        actor1_country_code="DEU", actor2_country_code="UKR",
        event_code="042", event_base_code="042", event_root_code="04",
    )
    df = parse_events(row.encode())
    assert df["event_code"][0] == "042"
    assert df["event_root_code"][0] == "04"


def test_stray_quote_does_not_break_rows():
    # GDELT doesn't quote fields, but names and URLs sometimes contain a lone `"`.
    # With CSV quoting on, the parser swallows everything up to the next quote.
    rows = [
        make_row(id="1", raw_date="20261004", actor1_country_code="USA",
                 actor2_country_code="CHN", actor1_name='THE "BIG',
                 source_url="https://example.com/a"),
        make_row(id="2", raw_date="20261004", actor1_country_code="FRA",
                 actor2_country_code="DEU", source_url="https://example.com/b"),
    ]
    df = parse_events("\n".join(rows).encode())
    assert df["id"].to_list() == ["1", "2"]


def test_empty_input_returns_empty_frame():
    df = parse_events(make_row(id="1", raw_date="20261004").encode())
    assert df.height == 0
    assert df.columns == EXPECTED_COLUMNS
