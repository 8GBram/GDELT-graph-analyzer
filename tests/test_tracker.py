from datetime import date

import pytest

from gdelt.tracker import IngestTracker


@pytest.fixture
def tracker(tmp_path):
    t = IngestTracker(tmp_path / "ingest.db")
    yield t
    t.close()


def test_creates_db_file(tmp_path):
    db = tmp_path / "sub" / "ingest.db"
    t = IngestTracker(db)
    t.close()
    assert db.exists()


def test_unknown_timestamp_has_no_status(tracker):
    assert tracker.status("20261003000000") is None


def test_record_and_read_back(tracker):
    tracker.record("20261003000000", "ok", rows=170)
    assert tracker.status("20261003000000") == "ok"
    assert tracker.rows("20261003000000") == 170


def test_missing_has_no_rows(tracker):
    tracker.record("20261003000000", "missing")
    assert tracker.status("20261003000000") == "missing"
    assert tracker.rows("20261003000000") is None


def test_recording_again_overwrites(tracker):
    # A retry turns 'failed' into 'ok': one row per timestamp, latest wins.
    tracker.record("20261003000000", "failed")
    tracker.record("20261003000000", "ok", rows=5)
    assert tracker.status("20261003000000") == "ok"
    assert tracker.summary(date(2026, 10, 3)) == {"ok": 1}


def test_rejects_unknown_status(tracker):
    with pytest.raises(ValueError):
        tracker.record("20261003000000", "done")


def test_summary_counts_statuses_for_that_day_only(tracker):
    tracker.record("20261003000000", "ok", rows=1)
    tracker.record("20261003001500", "ok", rows=2)
    tracker.record("20261003003000", "missing")
    tracker.record("20261003004500", "failed")
    tracker.record("20261004000000", "ok", rows=3)  # next day: not counted

    assert tracker.summary(date(2026, 10, 3)) == {"ok": 2, "missing": 1, "failed": 1}


def test_summary_of_untracked_day_is_empty(tracker):
    assert tracker.summary(date(2026, 10, 3)) == {}


def test_persists_across_connections(tmp_path):
    db = tmp_path / "ingest.db"
    first = IngestTracker(db)
    first.record("20261003000000", "ok", rows=1)
    first.close()

    second = IngestTracker(db)
    assert second.status("20261003000000") == "ok"
    second.close()
