from datetime import date

import polars as pl
import pytest
import requests

from gdelt import ingest
from tests.conftest import FakeResponse, export_zip

DAY = date(2026, 10, 3)


def ts(hhmm: str) -> str:
    return f"20261003{hhmm}00"


# --- day_timestamps ---------------------------------------------------------

def test_day_timestamps_covers_the_whole_day():
    stamps = ingest.day_timestamps(DAY)
    assert len(stamps) == 96
    assert stamps[0] == "20261003000000"
    assert stamps[1] == "20261003001500"
    assert stamps[-1] == "20261003234500"


def test_day_timestamps_are_unique_and_same_day():
    stamps = ingest.day_timestamps(DAY)
    assert len(set(stamps)) == 96
    assert all(s.startswith("20261003") for s in stamps)


# --- ingest_day -------------------------------------------------------------

def test_writes_parquet_named_by_day(fake_gdelt, tmp_path):
    fake_gdelt.responses = {ts("0000"): FakeResponse(200, export_zip("1"))}
    path = ingest.ingest_day(DAY, tmp_path)
    assert path == tmp_path / "2026-10-03.parquet"
    assert path.exists()


def test_requests_every_timestamp_of_the_day(fake_gdelt, tmp_path):
    fake_gdelt.responses = {ts("0000"): FakeResponse(200, export_zip("1"))}
    ingest.ingest_day(DAY, tmp_path)
    assert sorted(fake_gdelt.requested) == ingest.day_timestamps(DAY)


def test_combines_all_files_and_skips_404s(fake_gdelt, tmp_path):
    # Real GDELT has gaps: missing 15-minute files are skipped, not fatal.
    fake_gdelt.responses = {
        ts("0000"): FakeResponse(200, export_zip("1")),
        ts("1200"): FakeResponse(200, export_zip("2")),
        ts("2345"): FakeResponse(200, export_zip("3")),
    }
    path = ingest.ingest_day(DAY, tmp_path)
    df = pl.read_parquet(path)
    assert sorted(df["id"].to_list()) == ["1", "2", "3"]


def test_keeps_column_types_in_parquet(fake_gdelt, tmp_path):
    fake_gdelt.responses = {ts("0000"): FakeResponse(200, export_zip("1"))}
    df = pl.read_parquet(ingest.ingest_day(DAY, tmp_path))
    assert df.schema["date"] == pl.Date
    assert df["event_code"][0] == "042"


def test_creates_out_dir(fake_gdelt, tmp_path):
    fake_gdelt.responses = {ts("0000"): FakeResponse(200, export_zip("1"))}
    out_dir = tmp_path / "gdelt-data" / "events"
    path = ingest.ingest_day(DAY, out_dir)
    assert path.parent == out_dir
    assert path.exists()


def test_skips_download_when_file_exists(fake_gdelt, tmp_path):
    fake_gdelt.responses = {ts("0000"): FakeResponse(200, export_zip("1"))}
    first = ingest.ingest_day(DAY, tmp_path)
    fake_gdelt.requested.clear()

    second = ingest.ingest_day(DAY, tmp_path)

    assert second == first
    assert fake_gdelt.requested == []


def test_returns_none_and_writes_nothing_when_no_files_exist(fake_gdelt, tmp_path):
    # fake_gdelt returns 404 for every timestamp by default.
    assert ingest.ingest_day(DAY, tmp_path) is None
    assert list(tmp_path.rglob("*.parquet*")) == []


def test_server_error_raises_and_writes_nothing(fake_gdelt, tmp_path):
    # No file is better than a wrong file: a 503 must not leave a day with holes.
    fake_gdelt.responses = {
        ts("0000"): FakeResponse(200, export_zip("1")),
        ts("0015"): FakeResponse(503),
    }
    with pytest.raises(requests.HTTPError):
        ingest.ingest_day(DAY, tmp_path)
    assert list(tmp_path.rglob("*.parquet*")) == []  # no .parquet, no leftover .tmp


def test_retry_after_server_error_succeeds(fake_gdelt, tmp_path):
    fake_gdelt.responses = {
        ts("0000"): FakeResponse(200, export_zip("1")),
        ts("0015"): FakeResponse(503),
    }
    with pytest.raises(requests.HTTPError):
        ingest.ingest_day(DAY, tmp_path)

    fake_gdelt.responses[ts("0015")] = FakeResponse(200, export_zip("2"))
    df = pl.read_parquet(ingest.ingest_day(DAY, tmp_path))
    assert sorted(df["id"].to_list()) == ["1", "2"]


# --- ingest_day with a tracker ------------------------------------------------

@pytest.fixture
def tracker(tmp_path):
    from gdelt.tracker import IngestTracker

    t = IngestTracker(tmp_path / "ingest.db")
    yield t
    t.close()


def test_tracker_records_every_timestamp(fake_gdelt, tmp_path, tracker):
    fake_gdelt.responses = {ts("0000"): FakeResponse(200, export_zip("1"))}
    ingest.ingest_day(DAY, tmp_path / "events", tracker=tracker)

    assert tracker.summary(DAY) == {"ok": 1, "missing": 95}
    assert tracker.status(ts("0000")) == "ok"
    assert tracker.rows(ts("0000")) == 1
    assert tracker.status(ts("0015")) == "missing"


def test_tracker_records_failures_before_raising(fake_gdelt, tmp_path, tracker):
    fake_gdelt.responses = {
        ts("0000"): FakeResponse(200, export_zip("1")),
        ts("0015"): FakeResponse(503),
    }
    with pytest.raises(requests.HTTPError):
        ingest.ingest_day(DAY, tmp_path / "events", tracker=tracker)

    assert tracker.status(ts("0015")) == "failed"
    assert tracker.status(ts("0000")) == "ok"
    assert list((tmp_path / "events").rglob("*.parquet*")) == []


def test_tracker_updates_after_retry(fake_gdelt, tmp_path, tracker):
    fake_gdelt.responses = {
        ts("0000"): FakeResponse(200, export_zip("1")),
        ts("0015"): FakeResponse(503),
    }
    with pytest.raises(requests.HTTPError):
        ingest.ingest_day(DAY, tmp_path / "events", tracker=tracker)

    fake_gdelt.responses[ts("0015")] = FakeResponse(200, export_zip("2"))
    ingest.ingest_day(DAY, tmp_path / "events", tracker=tracker)

    assert tracker.summary(DAY) == {"ok": 2, "missing": 94}
