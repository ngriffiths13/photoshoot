import pytest

from photoshoot.core import PolarsLocalSnapshotTest


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

    parser.addini("polars-snapshot-gcs-location", type="string", help="GCS location")


@pytest.fixture
def pl_local_snapshot(request: pytest.FixtureRequest) -> PolarsLocalSnapshotTest:
    """Fixture to create a PolarsLocalSnapshotTest instance.

    Args:
    ----
        request: pytest request object

    Returns:
    -------
        PolarsLocalSnapshotTest: PolarsLocalSnapshotTest instance

    """
    test_name = request.node.name
    return PolarsLocalSnapshotTest(
        test_name=test_name,
        update_snapshot=request.config.option.new_snapshot,
    )


# @pytest.fixture
# def pl_gcs_snapshot(request: pytest.FixtureRequest):
#     gcs_uri = request.config.getini("polars-snapshot-gcs-location")
#     validate_gcs_uri(gcs_uri)
