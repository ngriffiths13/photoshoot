"""Google Cloud Storage storage for Polars DataFrames."""

import importlib.util
from pathlib import Path

import polars as pl

if importlib.util.find_spec("gcsfs") is not None:
    from gcsfs import GCSFileSystem


class GcsPolarsStorage:
    """Local storage for Polars DataFrames."""

    _FILE_EXTENSION = ".parquet"

    def __init__(
        self,
        gcs_path: str | Path,
        client: GCSFileSystem | None = None,
    ) -> None:
        """Initialize the LocalPolarsStorage.

        Args:
        ----
            gcs_path (str): The GCS URI to store the data.
            client (GCSFileSystem | None, optional): The GCSFileSystem client to use.

        """
        self.gcs_path = Path(gcs_path)
        self.client = client if client is not None else GCSFileSystem()

    def write(self, data: pl.DataFrame, file_name: str) -> None:
        """Write the data to a file.

        Args:
        ----
            data (pl.DataFrame): The data to write.
            file_name (str): The name of the file to write the data to.

        """
        with self.client.open(
            self.gcs_path / (file_name + self._FILE_EXTENSION), "wb"
        ) as f:
            data.write_parquet(f)

    def read(self, file_name: str) -> pl.DataFrame:
        """Read the data from a file.

        Args:
        ----
            file_name (str): The name of the file to read the data from.

        Raises:
        ------
            FileNotFoundError: If the file does not exist.

        Returns:
        -------
            DataFrame: The data read from the file.

        """
        file_path = self.gcs_path / (file_name + self._FILE_EXTENSION)
        if not self.client.exists(file_path):
            raise FileNotFoundError(f"No data found in {file_path}")
        with self.client.open(file_path, "rb") as f:
            return pl.read_parquet(f)
