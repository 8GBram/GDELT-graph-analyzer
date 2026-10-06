from datetime import date, datetime, timezone

import polars as pl
import pytest

from gdelt.edges import write_edges
from tests.conftest import event, events


def published(day: int) -> datetime:
    return datetime(2026, 10, day, 9, 0, tzinfo=timezone.utc)


@pytest.fixture
def events_dir(tmp_path):
    """Two daily events files, like `gdelt ingest` writes them."""
    d = tmp_path / "events"
    d.mkdir()
    events(
        event("1", "RUS", "UKR", quad_class=4, happened=date(2026, 10, 2), published=published(2)),
        event("2", "RUS", "UKR", quad_class=4, happened=date(2026, 10, 2), published=published(2)),
        event("3", "USA", "USA", happened=date(2026, 10, 2), published=published(2)),  # self-loop
    ).write_parquet(d / "2026-10-02.parquet")
    events(
        event("4", "DEU", "UKR", quad_class=1, happened=date(2026, 10, 3), published=published(3)),
        event("5", "EUR", "RUS", happened=date(2026, 10, 3), published=published(3)),  # region
    ).write_parquet(d / "2026-10-03.parquet")
    return d


def test_writes_cleaned_edges_from_all_files(events_dir, tmp_path):
    out = tmp_path / "edges" / "edges.parquet"
    write_edges(events_dir, out)

    written = pl.read_parquet(out)
    assert written.select("day", "source", "target", "quad_class", "n_events").rows() == [
        (date(2026, 10, 2), "RUS", "UKR", 4, 2),
        (date(2026, 10, 3), "DEU", "UKR", 1, 1),
    ]


def test_returns_what_it_wrote(events_dir, tmp_path):
    out = tmp_path / "edges.parquet"
    returned = write_edges(events_dir, out)
    assert returned.equals(pl.read_parquet(out))


def test_creates_output_folder(events_dir, tmp_path):
    out = tmp_path / "a" / "b" / "edges.parquet"
    write_edges(events_dir, out)
    assert out.exists()


def test_leaves_no_temp_file(events_dir, tmp_path):
    out_dir = tmp_path / "edges"
    write_edges(events_dir, out_dir / "edges.parquet")
    assert [p.name for p in out_dir.iterdir()] == ["edges.parquet"]


def test_rebuilds_from_scratch_on_each_run(events_dir, tmp_path):
    out = tmp_path / "edges.parquet"
    write_edges(events_dir, out)

    events(
        event("6", "CHN", "USA", happened=date(2026, 10, 4), published=published(4)),
    ).write_parquet(events_dir / "2026-10-04.parquet")
    write_edges(events_dir, out)

    assert pl.read_parquet(out)["day"].unique().sort().to_list() == [
        date(2026, 10, 2), date(2026, 10, 3), date(2026, 10, 4),
    ]


def test_passes_max_lag_days_to_cleaning(tmp_path):
    d = tmp_path / "events"
    d.mkdir()
    # Happened Oct 2, published Oct 3: a lag of 1 day.
    events(
        event("1", "DEU", "UKR", happened=date(2026, 10, 2), published=published(3)),
    ).write_parquet(d / "2026-10-03.parquet")

    assert write_edges(d, tmp_path / "lag0.parquet", max_lag_days=0).height == 0
    assert write_edges(d, tmp_path / "lag1.parquet", max_lag_days=1).height == 1


def test_ignores_leftover_temp_files(events_dir, tmp_path):
    # A crashed ingest could leave a .parquet.tmp behind; it must not be read.
    (events_dir / "2026-10-04.parquet.tmp").write_bytes(b"half a file")
    edges = write_edges(events_dir, tmp_path / "edges.parquet")
    assert edges["day"].max() == date(2026, 10, 3)


@pytest.mark.parametrize("make_dir", [True, False])
def test_no_events_files_raises(tmp_path, make_dir):
    d = tmp_path / "events"
    if make_dir:
        d.mkdir()
    with pytest.raises(FileNotFoundError):
        write_edges(d, tmp_path / "edges.parquet")
    assert not (tmp_path / "edges.parquet").exists()
