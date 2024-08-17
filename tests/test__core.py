from pathlib import Path

import polars as pl
import pytest

from photoshoot.core import PolarsLocalSnapshotTest, SnapshotTestFailedError


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
    d.rmdir()


@pytest.fixture
def patch_snapshot_dir(monkeypatch, temp_snapshot_dir):
    monkeypatch.setattr(
        PolarsLocalSnapshotTest, "_DEFAULT_DIRECTORY", temp_snapshot_dir
    )
    return temp_snapshot_dir


def test__polars_local_snapshot_test__new_snapshot(patch_snapshot_dir):
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    df = pl.DataFrame({"a": [1, 2, 3]})
    snapshot(df)


def test__polars_local_snapshot_test__snapshots_match(patch_snapshot_dir):
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    df = pl.DataFrame({"a": [1, 2, 3]})
    snapshot(df)
    snapshot = PolarsLocalSnapshotTest("test_name")
    snapshot(df)


def test__polars_local_snapshot_test__snapshots_dont_match(patch_snapshot_dir):
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    df = pl.DataFrame({"a": [1, 2, 3]})
    snapshot(df)
    df = pl.DataFrame({"a": [1, 2, 4]})
    snapshot = PolarsLocalSnapshotTest("test_name")
    with pytest.raises(SnapshotTestFailedError):
        snapshot(df)


def test__polars_local_snapshot_test__snapshots_dont_match__update_snapshot(
    patch_snapshot_dir,
):
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    df = pl.DataFrame({"a": [1, 2, 3]})
    snapshot(df)
    df = pl.DataFrame({"a": [1, 2, 4]})
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    snapshot(df)


def test__polars_local_snapshot_test__no_snapshot_to_compare(patch_snapshot_dir):
    snapshot = PolarsLocalSnapshotTest("test_name")
    df = pl.DataFrame({"a": [1, 2, 3]})
    with pytest.raises(SnapshotTestFailedError):
        snapshot(df)


def test__polars_local_snapshot_test__default_to_test_name(patch_snapshot_dir):
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    df = pl.DataFrame({"a": [1, 2, 3]})
    snapshot(df)
    assert (patch_snapshot_dir / "test_name.parquet").exists()


def test__polars_local_snapshot_test__explicit_name_passed(patch_snapshot_dir: Path):
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    df = pl.DataFrame({"a": [1, 2, 3]})
    snapshot(df, name="my_snapshot")
    assert not (patch_snapshot_dir / "test_name.parquet").exists()
    assert (patch_snapshot_dir / "my_snapshot.parquet").exists()


def test__polars_local_snapshot_test__pass_kwargs(patch_snapshot_dir: Path):
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=True)
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    snapshot(df)
    snapshot = PolarsLocalSnapshotTest("test_name", update_snapshot=False)
    df = pl.DataFrame({"b": [4, 5, 6], "a": [1, 2, 3]})
    snapshot(df, assert_kwargs={"check_column_order": False})
