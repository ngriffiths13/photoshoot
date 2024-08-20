from photoshoot.comp import PolarsPhotoshootCompare
import polars as pl
from polars.testing import assert_frame_equal, assert_frame_not_equal


def test__polars_photoshoot_comp__default_init():
    comp = PolarsPhotoshootCompare()
    assert comp.assert_frame_equal_kwargs == {}
    assert comp.equality_func.func == assert_frame_equal


def test__polars_photoshoot_comp__not_equal_init():
    comp = PolarsPhotoshootCompare(is_equal=False)
    assert comp.assert_frame_equal_kwargs == {}
    assert comp.equality_func.func == assert_frame_not_equal


def test__polars_photoshoot_comp__init_with_kwargs():
    comp = PolarsPhotoshootCompare(
        assert_frame_equal_kwargs={"check_column_order": False}
    )
    assert comp.assert_frame_equal_kwargs == {"check_column_order": False}
    assert comp.equality_func.func == assert_frame_equal


def test__polars_photoshoot_comp__not_equal_init_with_kwargs():
    comp = PolarsPhotoshootCompare(
        is_equal=False, assert_frame_equal_kwargs={"check_column_order": False}
    )
    assert comp.assert_frame_equal_kwargs == {"check_column_order": False}
    assert comp.equality_func.func == assert_frame_not_equal


def test__polars_photoshoot_comp__compare():
    comp = PolarsPhotoshootCompare()
    df1 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    df2 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    comp.compare(df1, df2)
