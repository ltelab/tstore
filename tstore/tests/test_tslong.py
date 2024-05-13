"""Test the tslong subpackage."""

import os
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

import tstore
from tstore.tslong.pandas import TSLongPandas
from tstore.tslong.polars import TSLongPolars

# Imported fixtures from conftest.py:
# - pandas_long_dataframe
# - polars_long_dataframe


# Functions ####################################################################


def store_tslong(tslong: tstore.TSLong, dirpath: str) -> None:
    tstore_structure = "id-var"
    overwrite = True
    id_var = "store_id"
    time_var = "time"
    static_variables = ["static_var"]
    # geometry = None  # NOT IMPLEMENTED YET

    # Same partitioning for all TS
    partitioning = "year/month"
    # Partitioning specific to each TS
    partitioning = {"precipitation": "year/month"}

    # Each timeseries is a TS object
    ts_variables = ["name", "id", "x", "y"]
    # Group multiple timeseries into one TS object
    ts_variables = {"precipitation": ["name", "id", "x", "y"]}

    tslong.to_tstore(
        # TSTORE options
        dirpath,
        # DFLONG attributes
        id_var=id_var,
        time_var=time_var,
        ts_variables=ts_variables,
        static_variables=static_variables,
        # TSTORE options
        partitioning=partitioning,
        tstore_structure=tstore_structure,
        overwrite=overwrite,
    )


# Tests ########################################################################


@pytest.mark.parametrize(
    "dataframe_fixture_name",
    [
        "pandas_long_dataframe",
        "polars_long_dataframe",
    ],
)
def test_store(
    tmp_path: Path,
    dataframe_fixture_name: str,
    request,
) -> None:
    """Test the to_tstore function."""
    df = request.getfixturevalue(dataframe_fixture_name)
    tslong = tstore.TSLong.wrap(df)
    dirpath = tmp_path / "test_tstore"
    store_tslong(tslong, str(dirpath))

    # Check that the tstore is created
    assert dirpath.is_dir()

    # Check directory content
    assert sorted(os.listdir(dirpath)) == ["1", "2", "3", "4", "_attributes.parquet", "tstore_metadata.yml"]
    assert os.listdir(dirpath / "1" / "precipitation" / "year=2000" / "month=1") == ["part-0.parquet"]


class TestLoad:
    """Test the from_tstore function."""

    def test_pandas(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a Pandas DataFrame."""
        tslong = tstore.open_tslong(tstore_path, backend="pandas", ts_variables=["precipitation"])
        assert type(tslong) is TSLongPandas
        assert type(tslong._df) is pd.DataFrame
        assert tslong.shape == (192, 7)
        # TODO: dataframe should be wrapped in a TSLong object
        # TODO: time column is counted
        # TODO: line order is not preserved
        # TODO: column order is not preserved

    def test_polars(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a Polars DataFrame."""
        tslong = tstore.open_tslong(tstore_path, backend="polars", ts_variables=["precipitation"])
        assert type(tslong) is TSLongPolars
        assert type(tslong._df) is pl.DataFrame
        assert tslong.shape == (192, 7)
        # TODO: dataframe should be wrapped in a TSLong object
        # TODO: time column is counted
        # TODO: line order is not preserved
        # TODO: column order is not preserved
