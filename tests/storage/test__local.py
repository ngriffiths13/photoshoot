import pytest
import polars as pl
from photoshoot.storage import LocalPolarsStorage
from pathlib import Path
from polars.testing import assert_frame_equal


@pytest.fixture
def temp_snapshot_dir(tmp_path) -> Path:
    d = tmp_path / ".snapshots"
    d.mkdir()
    yield d
    # cleanup
    # empty the directory
    for child in d.iterdir():
        if child.is_file():
            child.unlink()
        else:
            for grandchild in child.iterdir():
                grandchild.unlink()
            child.rmdir()
    d.rmdir()


def test__local_polars_storage__write(temp_snapshot_dir):
    storage = LocalPolarsStorage(temp_snapshot_dir)
    df = pl.DataFrame(
        {
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
        }
    )
    storage.write(df, "test")
    assert (temp_snapshot_dir / "test.parquet").exists()


def test__local_polars_storage__read(temp_snapshot_dir):
    storage = LocalPolarsStorage(temp_snapshot_dir)
    df = pl.DataFrame(
        {
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
        }
    )
    storage.write(df, "test")
    assert (temp_snapshot_dir / "test.parquet").exists()
    df2 = storage.read("test")
    assert_frame_equal(df, df2)


def test__local_polars_storage__no_file_rasies(temp_snapshot_dir):
    storage = LocalPolarsStorage(temp_snapshot_dir)
    with pytest.raises(FileNotFoundError, match=f"No data found in {temp_snapshot_dir / 'test.parquet'}"):
        storage.read("test")
