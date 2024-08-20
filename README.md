# photoshoot

Photoshoot is a pytest plugin libary for making snapshot testing with Polars DataFrames easy.

## Installation

```bash
pip install photoshoot
```

## Usage

On the first run of this test, it will fail. Because no previous snapshot has been created.
```python
import pytest
import polars as pl

def test_dataframe(local_snapshot):
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
    })
    local_snapshot(df)
```

Running the test like this, will create a snapshot of the dataframe. All future runs will compare the dataframe with the snapshot.
```bash
pytest --new-snapshot
```

Rerunning the test now will pass, as the snapshot is already created.

## Features
Currenty this library only supports Polars DataFrames, and only supports local snapshots or gcs snapshots.
