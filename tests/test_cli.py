from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from gdelt import cli
from gdelt.ingest import IngestResult
from tests.conftest import event, events


@pytest.fixture
def fake_ingest_range(monkeypatch):
    """Replace ingest_range so CLI tests don't download anything.

    Records the arguments it was called with; set `result` to control what it returns.
    """
    calls = []
    state = {"result": IngestResult()}

    def fake(start, end, out_dir, max_workers=8, tracker=None):
        calls.append({"start": start, "end": end, "out_dir": out_dir, "max_workers": max_workers})
        return state["result"]

    monkeypatch.setattr(cli, "ingest_range", fake)
    return calls, state


# --- gdelt ingest -------------------------------------------------------------

def test_ingest_one_date(fake_ingest_range, tmp_path):
    calls, _ = fake_ingest_range
    code = cli.main(["ingest", "--date", "2026-10-03", "--data-dir", str(tmp_path)])
    assert code == 0
    assert calls == [{"start": date(2026, 10, 3), "end": date(2026, 10, 3),
                      "out_dir": tmp_path / "events", "max_workers": 8}]


def test_ingest_range_and_workers(fake_ingest_range, tmp_path):
    calls, _ = fake_ingest_range
    cli.main(["ingest", "--start", "2026-09-01", "--end", "2026-09-30",
              "--workers", "4", "--data-dir", str(tmp_path)])
    assert calls[0]["start"] == date(2026, 9, 1)
    assert calls[0]["end"] == date(2026, 9, 30)
    assert calls[0]["max_workers"] == 4


def test_ingest_yesterday_is_utc(fake_ingest_range, tmp_path):
    calls, _ = fake_ingest_range
    cli.main(["ingest", "--yesterday", "--data-dir", str(tmp_path)])
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    assert calls[0]["start"] == calls[0]["end"] == yesterday


def test_ingest_exit_code_1_when_a_day_failed(fake_ingest_range, tmp_path):
    _, state = fake_ingest_range
    state["result"] = IngestResult(failed=[date(2026, 10, 3)])
    assert cli.main(["ingest", "--date", "2026-10-03", "--data-dir", str(tmp_path)]) == 1


@pytest.mark.parametrize("argv", [
    ["ingest"],                                                   # no date given
    ["ingest", "--start", "2026-09-01"],                          # --start without --end
    ["ingest", "--end", "2026-09-01", "--date", "2026-09-01"],    # --end without --start
    ["ingest", "--date", "2026-10-03", "--yesterday"],            # two ways to pick dates
    ["ingest", "--date", "2026-13-01"],                           # not a real date
])
def test_ingest_bad_arguments_exit_2(fake_ingest_range, argv):
    calls, _ = fake_ingest_range
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2
    assert calls == []


def test_ingest_unfinished_day_exits_2(tmp_path):
    # The real ingest_range refuses days that aren't over yet; nothing is downloaded.
    today = datetime.now(timezone.utc).date().isoformat()
    assert cli.main(["ingest", "--date", today, "--data-dir", str(tmp_path)]) == 2


# --- gdelt edges --------------------------------------------------------------

@pytest.fixture
def data_dir(tmp_path):
    """A data folder with one day of downloaded events."""
    (tmp_path / "events").mkdir()
    events(
        event("1", "RUS", "UKR", quad_class=4),
        event("2", "DEU", "UKR", quad_class=1),
    ).write_parquet(tmp_path / "events" / "2026-10-03.parquet")
    return tmp_path


def test_edges_writes_edges_file(data_dir):
    assert cli.main(["edges", "--data-dir", str(data_dir)]) == 0
    edges = pl.read_parquet(data_dir / "edges" / "edges.parquet")
    assert edges.select("source", "target").rows() == [("DEU", "UKR"), ("RUS", "UKR")]


def test_edges_passes_max_lag_days(monkeypatch, data_dir):
    seen = {}

    def fake_write_edges(events_dir, out_path, max_lag_days=3):
        seen.update(events_dir=events_dir, out_path=out_path, max_lag_days=max_lag_days)
        return pl.DataFrame({"day": [], "source": [], "target": []},
                            schema={"day": pl.Date, "source": pl.String, "target": pl.String})

    monkeypatch.setattr(cli, "write_edges", fake_write_edges)
    cli.main(["edges", "--data-dir", str(data_dir), "--max-lag-days", "7"])
    assert seen == {"events_dir": data_dir / "events",
                    "out_path": data_dir / "edges" / "edges.parquet",
                    "max_lag_days": 7}


def test_edges_without_events_exits_1(tmp_path):
    assert cli.main(["edges", "--data-dir", str(tmp_path)]) == 1
    assert not (tmp_path / "edges").exists()


def test_edges_negative_lag_exits_2(data_dir):
    with pytest.raises(SystemExit) as exc:
        cli.main(["edges", "--data-dir", str(data_dir), "--max-lag-days", "-1"])
    assert exc.value.code == 2


# --- shared options -------------------------------------------------------------

def test_data_dir_defaults_to_env_var(monkeypatch, data_dir):
    monkeypatch.setenv("GDELT_DATA_DIR", str(data_dir))
    assert cli.main(["edges"]) == 0
    assert (data_dir / "edges" / "edges.parquet").exists()


def test_no_subcommand_exits_2():
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 2
