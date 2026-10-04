import pytest
import requests

from gdelt import ingest
from gdelt.ingest import download_export
from tests.conftest import FakeResponse, zip_bytes


@pytest.fixture
def fake_get(monkeypatch):
    """Replace requests.get inside ingest; records the URLs it was called with."""
    calls = []

    def install(response: FakeResponse):
        def get(url, **kwargs):
            calls.append((url, kwargs))
            return response

        monkeypatch.setattr(ingest.requests, "get", get)
        return calls

    return install


def test_builds_correct_url(fake_get):
    calls = fake_get(FakeResponse(200, zip_bytes("x.CSV", b"data")))
    download_export("20261004080000")
    url, _ = calls[0]
    assert url == "https://data.gdeltproject.org/gdeltv2/20261004080000.export.CSV.zip"


def test_sets_a_timeout(fake_get):
    calls = fake_get(FakeResponse(200, zip_bytes("x.CSV", b"data")))
    download_export("20261004080000")
    _, kwargs = calls[0]
    assert kwargs.get("timeout") is not None


def test_returns_unzipped_bytes(fake_get):
    fake_get(FakeResponse(200, zip_bytes("20261004080000.export.CSV", b"a\tb\n")))
    assert download_export("20261004080000") == b"a\tb\n"


def test_returns_none_on_404(fake_get):
    # The newest file in lastupdate.txt is often listed before it's downloadable.
    fake_get(FakeResponse(404, b"<html>Not Found</html>"))
    assert download_export("20261004084500") is None


@pytest.mark.parametrize("status", [429, 500, 503])
def test_raises_on_server_error(fake_get, status):
    # A temporary failure must not look like a missing file, or the day gets saved with holes.
    fake_get(FakeResponse(status))
    with pytest.raises(requests.HTTPError):
        download_export("20261004080000")


@pytest.mark.network
def test_real_download():
    csv_bytes = download_export("20261004080000")
    assert csv_bytes is not None
    first_row = csv_bytes.split(b"\n", 1)[0]
    assert first_row.count(b"\t") == 60
