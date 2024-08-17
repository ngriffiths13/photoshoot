"""Core classes and functions for polars snapshots."""

from pathlib import Path
from typing import Any

import polars as pl
from polars.testing import assert_frame_equal


class SnapshotTestFailedError(Exception):
    """Exception raised when a snapshot test fails."""

    ...


class PolarsLocalSnapshotTest:
    """Class to handle local snapshots for tests.

    This class handles the creation and comparison of snapshots for tests.
    Currently this class is only focused on handling polars DataFrames.
    """

    _DEFAULT_DIRECTORY = Path(__file__).parent.joinpath(".snapshots")
    _FILE_EXTENSION = ".parquet"

    def __init__(
        self,
        test_name: str,
        update_snapshot: bool = False,
    ) -> None:
        """Initialize the LocalSnapshot class.

        Args:
        ----
            test_name (str): The name of the test.
            update_snapshot (bool, optional): Whether to update the snapshot.
                If true, tests automatically pass. Defaults to False.

        """
        self.update_snapshot = update_snapshot
        self.test_name = test_name

    def __call__(
        self,
        df: pl.DataFrame,
        name: str | None = None,
        assert_kwargs: dict[str, str] | None = None,
    ) -> None:
        """Create or compare a snapshot.

        Args:
        ----
            df (pl.DataFrame): The DataFrame from the test.
            name (str, optional): The name of the snapshot. Defaults to the name
                of the test.
            assert_kwargs (dict[str, str], optional): The kwargs to pass to the
                assert_frame_equal function. Defaults to None.

        Raises:
        ------
            SnapshotTestFailedError: Raised when the snapshot test fails.

        """
        if name is None:
            name = self.test_name

        if assert_kwargs is None:
            assert_kwargs = {}

        # If we are updating the snapshot, write the file and return. Test should
        # always pass.
        if self.update_snapshot:
            self._write_file(df, name)
            return

        # Read file in as DataFrame.
        try:
            snapshot_df = self._read_file(name)
        except FileNotFoundError:
            raise SnapshotTestFailedError(
                "No snapshot found. Test failing.",
                "Pass --take-new-snapshot to create a new snapshot.",
            )

        # Compare the DataFrames
        try:
            self._compare(df, snapshot_df, assert_kwargs)
        except AssertionError as e:
            raise SnapshotTestFailedError(f"Snapshot test failed for {name}.") from e

    def _write_file(self, df: pl.DataFrame, name: str) -> None:
        if not (self._DEFAULT_DIRECTORY).exists():
            (self._DEFAULT_DIRECTORY).mkdir(parents=True)
        df.write_parquet(self._DEFAULT_DIRECTORY / (name + self._FILE_EXTENSION))

    def _read_file(self, name: str) -> pl.DataFrame:
        file_path = self._DEFAULT_DIRECTORY / (name + self._FILE_EXTENSION)
        if not file_path.exists():
            raise FileNotFoundError(
                "No snapshot found. Test failing.",
                "Pass --take-new-snapshot to create a new snapshot.",
            )
        return pl.read_parquet(self._DEFAULT_DIRECTORY / (name + self._FILE_EXTENSION))

    def _compare(
        self, df: pl.DataFrame, snapshot_df: pl.DataFrame, assert_kwargs: dict[str, Any]
    ) -> None:
        assert_frame_equal(df, snapshot_df, **assert_kwargs)
