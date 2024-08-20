"""Core classes and functions for polars snapshots."""

import importlib.util
from typing import Any, Protocol, TypeVar

import polars as pl

if importlib.util.find_spec("gcsfs") is not None:
    pass


DataType = TypeVar("DataType", Any, pl.DataFrame)


class SnapshotTestFailedError(Exception):
    """Exception raised when a snapshot test fails."""

    ...


class PhotoshootStorage(Protocol):
    """Protocol for a storage class that can read and write snapshots."""

    def write(self, data: DataType, file_name: str) -> None:
        """Write the data to storage."""
        ...

    def read(self, file_name: str) -> DataType:
        """Read the data from storage."""
        ...


class PhotoshootCompare(Protocol):
    """Protocol for a class that can compare two data types."""

    def compare(self, data: DataType, other: DataType) -> None:
        """Compare two data types."""
        ...


class PhotoshootTest:
    """Class to handle local snapshots for tests.

    This class handles the creation and comparison of snapshots for tests.
    Currently this class is only focused on handling polars DataFrames.
    """

    def __init__(
        self,
        test_name: str,
        storage: PhotoshootStorage,
        compare: PhotoshootCompare,
        update_snapshot: bool = False,
    ) -> None:
        """Initialize the LocalSnapshot class.

        Args:
        ----
            test_name (str): The name of the test.
            storage (PhotoshootStorage): The storage object to use for reading and
                writing snapshots.
            compare (PhotoshootCompare): The compare object to use for comparing
                snapshots.
            update_snapshot (bool, optional): Whether to update the snapshot.
                If true, tests automatically pass. Defaults to False.

        """
        self.update_snapshot = update_snapshot
        self.test_name = test_name
        self.storage = storage
        self.compare = compare

    def __call__(
        self,
        df: pl.DataFrame,
        name: str | None = None,
    ) -> None:
        """Create or compare a snapshot.

        Args:
        ----
            df (pl.DataFrame): The DataFrame from the test.
            name (str, optional): The name of the snapshot. Defaults to the name
                of the test.

        Raises:
        ------
            SnapshotTestFailedError: Raised when the snapshot test fails.

        """
        name = self.test_name if name is None else f"{self.test_name}/{name}"

        # If we are updating the snapshot, write the file and return. Test should
        # always pass.
        if self.update_snapshot:
            self.storage.write(df, name)
            return

        # Read file in as DataFrame.
        try:
            snapshot_df = self.storage.read(name)
        except FileNotFoundError:
            raise SnapshotTestFailedError(
                "No snapshot found. Test failing.",
                "Pass --take-new-snapshot to create a new snapshot.",
            )

        # Compare the DataFrames
        try:
            self.compare.compare(df, snapshot_df)
        except AssertionError as e:
            raise SnapshotTestFailedError(f"Snapshot test failed for {name}.") from e
