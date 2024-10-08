"""Test the TS class."""

from pathlib import Path

import dask
import pandas as pd
import polars as pl
import polars.testing as pl_testing
import pytest

from tstore.tsdf.ts_class import TS

# Imported fixtures from conftest.py:
# - dask_dataframe
# - pandas_dataframe
# - polars_dataframe


dataframe_fixture_names = [
    "dask_dataframe",
    "pandas_dataframe",
    "polars_dataframe",
]


@pytest.mark.parametrize(
    "dataframe_fixture_name",
    dataframe_fixture_names,
)
def test_wrap(
    dataframe_fixture_name: str,
    request,
) -> None:
    """Test the TS wrapper instantiation."""
    df = request.getfixturevalue(dataframe_fixture_name)
    ts = TS(df)

    assert dir(ts._obj) == dir(df)
    assert ts._tstore_time_var == "time"
    assert ts.current_backend == dataframe_fixture_name.split("_")[0]


@pytest.mark.parametrize(
    "dataframe_fixture_name",
    dataframe_fixture_names,
)
def test_store(
    dataframe_fixture_name: str,
    request,
    tmp_path: Path,
) -> None:
    """Test the to_disk method of the TS class."""
    filepath = str(tmp_path / "test.parquet")
    df = request.getfixturevalue(dataframe_fixture_name)
    ts = TS(df)
    ts.to_disk(filepath)

    assert Path(filepath).exists()


class TestLoad:
    """Test the from_disk method of the TS class."""

    def test_dask(
        self,
        parquet_timeseries: Path,
    ) -> None:
        """Test on a Dask TS object."""
        ts = TS.from_disk(parquet_timeseries, partitions=[], backend="dask")
        assert isinstance(ts._obj, dask.dataframe.DataFrame)

    def test_pandas(
        self,
        parquet_timeseries: Path,
    ) -> None:
        """Test on a Pandas TS object."""
        ts = TS.from_disk(parquet_timeseries, partitions=[], backend="pandas")
        assert isinstance(ts._obj, pd.DataFrame)

    def test_polars(
        self,
        parquet_timeseries: Path,
    ) -> None:
        """Test on a Polars TS object."""
        ts = TS.from_disk(parquet_timeseries, partitions=[], backend="polars")
        assert isinstance(ts._obj, pl.DataFrame)


class TestStoreAndLoad:
    """Test that the to_disk and from_disk methods of the TS class are consistent."""

    def test_dask(
        self,
        dask_dataframe: dask.dataframe.DataFrame,
        tmp_path: Path,
    ) -> None:
        """Test on a Dask TS object."""
        filepath = str(tmp_path / "test.parquet")
        ts = TS(dask_dataframe)
        ts.to_disk(filepath)
        ts_loaded = TS.from_disk(filepath, partitions=[], backend="dask")

        df = ts._obj.compute()
        df_loaded = ts_loaded._obj.compute()

        # Test equality without matching types
        # index is datetime or timestamp, strings are string or large_string
        pd.testing.assert_frame_equal(df, df_loaded, check_dtype=False, check_index_type=False, check_freq=False)

    def test_pandas(
        self,
        pandas_dataframe: pd.DataFrame,
        tmp_path: Path,
    ) -> None:
        """Test on a Pandas TS object."""
        filepath = str(tmp_path / "test.parquet")
        ts = TS(pandas_dataframe)
        ts.to_disk(filepath)
        ts_loaded = TS.from_disk(filepath, partitions=[], backend="pandas")

        df = ts._obj
        df_loaded = ts_loaded._obj

        # Test equality without matching types
        # index is datetime or timestamp, strings are string or large_string
        pd.testing.assert_frame_equal(df, df_loaded, check_dtype=False, check_index_type=False, check_freq=False)

    def test_polars(
        self,
        polars_dataframe: pl.DataFrame,
        tmp_path: Path,
    ) -> None:
        """Test on a Pandas TS object."""
        filepath = str(tmp_path / "test.parquet")

        ts = TS(polars_dataframe)
        ts.to_disk(filepath)
        ts_loaded = TS.from_disk(filepath, partitions=[], backend="polars")

        df = ts._obj
        df_loaded = ts_loaded._obj

        # Test equality without matching types
        pl_testing.assert_frame_equal(df, df_loaded, check_column_order=False)
