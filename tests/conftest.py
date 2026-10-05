from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pytest
import requests

from gdelt import ingest
from gdelt.config import GDELT_SCHEMA

DATA_DIR = Path(__file__).parent / "data"


def make_row(**fields: str) -> str:
    """Build one tab-separated GDELT row. Unspecified columns are left empty."""
    unknown = set(fields) - set(GDELT_SCHEMA)
    assert not unknown, f"unknown columns: {unknown}"
    return "\t".join(fields.get(name, "") for name in GDELT_SCHEMA)


def zip_bytes(name: str, content: bytes) -> bytes:
    """Zip `content` in memory, the way GDELT serves its files."""
    buf = BytesIO()
    with ZipFile(buf, "w") as z:
        z.writestr(name, content)
    return buf.getvalue()


def export_zip(event_id: str) -> bytes:
    """A zipped export file containing one usable country-to-country event."""
    row = make_row(
        id=event_id, raw_date="20261003",
        actor1_country_code="DEU", actor2_country_code="UKR",
        event_code="042", event_base_code="042", event_root_code="04",
        date_added="20261003000000",
    )
    return zip_bytes(f"{event_id}.export.CSV", row.encode())


class FakeResponse:
    def __init__(self, status_code: int, content: bytes = b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)


class FakeGdelt:
    """Stands in for the GDELT server. Unknown timestamps return 404, like real gaps."""

    def __init__(self):
        self.responses: dict[str, FakeResponse] = {}
        self.requested: list[str] = []

    def get(self, url: str, **kwargs) -> FakeResponse:
        timestamp = url.rsplit("/", 1)[-1].split(".")[0]
        self.requested.append(timestamp)
        return self.responses.get(timestamp, FakeResponse(404))


@pytest.fixture
def fake_gdelt(monkeypatch) -> FakeGdelt:
    fake = FakeGdelt()
    monkeypatch.setattr(ingest._session, "get", fake.get) 
    return fake


@pytest.fixture
def sample_bytes() -> bytes:
    """10 real rows from 20261004080000.export.CSV: 6 with both actor countries, 4 without."""
    return (DATA_DIR / "sample.export.CSV").read_bytes()
