import pytest

from photoshoot.comp import PolarsPhotoshootCompare
from photoshoot.core import PhotoshootTest
from photoshoot.storage import GcsPolarsStorage, LocalPolarsStorage


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom CLI options to pytest."""
    group = parser.getgroup("polars-snapshot-test")
    group.addoption(
        "--new-snapshot",
        action="store_true",
        dest="new_snapshot",
        default=False,
        help="Whether to take a new snapshot of a test. Defaults to False",
    )

    parser.addini(
        "photoshoot-gcs-path",
        type="string",
        help="GCS location to store snapshots. Should not start with 'gs://'",
    )


@pytest.fixture
def local_snapshot(request: pytest.FixtureRequest) -> PhotoshootTest:
    """Fixture to create a PolarsLocalSnapshotTest instance.

    Args:
    ----
        request: pytest request object

    Returns:
    -------
        PhotoshootTest: PhotoshootTest instance

    """
    test_name = request.node.name
    return PhotoshootTest(
        test_name=test_name,
        storage=LocalPolarsStorage(),
        compare=PolarsPhotoshootCompare(),
        update_snapshot=request.config.getoption("new_snapshot"),
    )


@pytest.fixture
def gcs_snapshot(request: pytest.FixtureRequest) -> PhotoshootTest:
    """Fixture to create a PolarsLocalSnapshotTest instance.

    Args:
    ----
        request: pytest request object

    Returns:
    -------
        PhotoshootTest: PhotoshootTest instance

    """
    test_name = request.node.name
    gcs_path = request.config.getini("photoshoot-gcs-path")
    return PhotoshootTest(
        test_name=test_name,
        storage=GcsPolarsStorage(gcs_path),
        compare=PolarsPhotoshootCompare(),
        update_snapshot=request.config.getoption("new_snapshot"),
    )
