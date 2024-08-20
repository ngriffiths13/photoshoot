"""Module containing implementations of the PhotoshootCompare Protocol."""

from functools import partial

import polars as pl
from polars.testing import assert_frame_equal, assert_frame_not_equal


class PolarsPhotoshootCompare:
    """Compare two DataFrames using Polars' assert_frame_equal/not_equal."""

    def __init__(
        self, assert_frame_equal_kwargs: dict | None = None, is_equal: bool = True
    ) -> None:
        """Initialize the PolarsPhotoshootCompare object."""
        self.assert_frame_equal_kwargs = (
            assert_frame_equal_kwargs if assert_frame_equal_kwargs is not None else {}
        )
        self.equality_func = (
            partial(assert_frame_equal, **self.assert_frame_equal_kwargs)
            if is_equal
            else partial(assert_frame_not_equal, **self.assert_frame_equal_kwargs)
        )

    def compare(self, df_1: pl.DataFrame, df_2: pl.DataFrame) -> None:
        """Compare two DataFrames using Polars' assert_frame_equal/not_equal.

        Args:
        ----
            df_1: The first DataFrame to compare.
            df_2: The second DataFrame to compare.


        """
        self.equality_func(df_1, df_2, **self.assert_frame_equal_kwargs)
