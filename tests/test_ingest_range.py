from datetime import date, datetime, timedelta, timezone

import pytest

from gdelt import ingest


@pytest.fixture
def fake_ingest_day(monkeypatch):
    """Replace ingest_day so these tests only check the looping logic, not downloads.

    Days listed in `empty_days` behave as if GDELT had no data for them.
    """
    calls: list[date] = []
    empty_days: set[date] = set()

    def fake(day, out_dir, *args, **kwargs):
        calls.append(day)
        return None if day in empty_days else out_dir / f"{day.isoformat()}.parquet"

    monkeypatch.setattr(ingest, "ingest_day", fake)
    return calls, empty_days


def test_calls_ingest_day_for_each_day_inclusive(fake_ingest_day, tmp_path):
    calls, _ = fake_ingest_day
    paths = ingest.ingest_range(date(2026, 9, 1), date(2026, 9, 3), tmp_path)
    assert calls == [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)]
    assert paths == [
        tmp_path / "2026-09-01.parquet",
        tmp_path / "2026-09-02.parquet",
        tmp_path / "2026-09-03.parquet",
    ]


def test_single_day_range(fake_ingest_day, tmp_path):
    calls, _ = fake_ingest_day
    ingest.ingest_range(date(2026, 9, 1), date(2026, 9, 1), tmp_path)
    assert calls == [date(2026, 9, 1)]


def test_leaves_out_days_without_data(fake_ingest_day, tmp_path):
    _, empty_days = fake_ingest_day
    empty_days.add(date(2026, 9, 2))
    paths = ingest.ingest_range(date(2026, 9, 1), date(2026, 9, 3), tmp_path)
    assert paths == [tmp_path / "2026-09-01.parquet", tmp_path / "2026-09-03.parquet"]


def test_refuses_unfinished_day(fake_ingest_day, tmp_path):
    # Today (UTC) isn't over: ingesting it would save a partial day that is never filled in.
    calls, _ = fake_ingest_day
    today = datetime.now(timezone.utc).date()
    with pytest.raises(ValueError):
        ingest.ingest_range(today - timedelta(days=2), today, tmp_path)
    assert calls == []  # check up front, before downloading anything


def test_refuses_start_after_end(fake_ingest_day, tmp_path):
    with pytest.raises(ValueError):
        ingest.ingest_range(date(2026, 9, 3), date(2026, 9, 1), tmp_path)
