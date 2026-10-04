from io import BytesIO
from zipfile import ZipFile
import polars as pl
import requests

# Example GDELT 2.0 Event URL (Zipped)
url = "https://data.gdeltproject.org/gdeltv2/20261004080000.export.CSV.zip"

# 1. Download the zip file bytes into memory
response = requests.get(url, timeout=30)
response.raise_for_status()

with ZipFile(BytesIO(response.content)) as z:
    csv_filename = z.namelist()[0]

    # Read the raw uncompressed bytes
    csv_bytes = z.read(csv_filename)

df = pl.read_csv(
    csv_bytes,
    separator="\t",
    has_header=False,
    infer_schema=False
)

print(df.shape)

print(df.head())
