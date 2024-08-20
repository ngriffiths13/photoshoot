from pathlib import Path

import polars as pl


class LocalPolarsStorage:
    """Local storage for Polars DataFrames."""

    _FILE_EXTENSION = ".parquet"

    def __init__(
        self,
        directory: str | Path = Path(__file__).parent / ".snapshots",
    ) -> None:
        """Initialize the LocalPolarsStorage.

        Args:
        ----
            directory (str | Path, optional): The directory to store the data. Defaults
                to Path(__file__).parent / "snapshots".

        """
        self.directory = Path(directory)

    def write(self, data: pl.DataFrame, file_name: str) -> None:
        """Write the data to a file.

        Args:
        ----
            data (pl.DataFrame): The data to write.
            file_name (str): The name of the file to write the data to.

        """
        file_path = self.directory / (file_name + self._FILE_EXTENSION)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        data.write_parquet(file_path)

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
        file_path = self.directory / (file_name + self._FILE_EXTENSION)
        if not file_path.exists():
            raise FileNotFoundError(f"No data found in {file_path}")
        return pl.read_parquet(self.directory / (file_name + self._FILE_EXTENSION))
