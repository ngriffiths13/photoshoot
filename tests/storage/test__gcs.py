from polars.testing.asserts.frame import assert_frame_equal
import pytest
from gcsfs import GCSFileSystem
from photoshoot.storage import GcsPolarsStorage
from pathlib import Path
import polars as pl


@pytest.fixture
def setup_test_bucket():
    gcs = GCSFileSystem()
    bucket_name = "test_bucket__photoshoot_testing"
    gcs.mkdir(bucket_name)
    yield bucket_name
    gcs.rm(bucket_name, recursive=True)


def test__gcs_polars_storage__init(setup_test_bucket):
    bucket_name = Path(setup_test_bucket) / "testing_data"
    storage = GcsPolarsStorage(bucket_name)
    assert str(storage.gcs_path) == f"{setup_test_bucket}/testing_data"


def test__gcs_polars_storage__write__read(setup_test_bucket):
    bucket_name = Path(setup_test_bucket) / "testing_data"
    storage = GcsPolarsStorage(bucket_name)
    data = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    storage.write(data, "test")
    out = storage.read("test")
    assert_frame_equal(data, out)
