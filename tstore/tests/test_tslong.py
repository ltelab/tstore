"""Test the tslong subpackage."""

import os
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

import tstore
from tstore.backend import Backend
from tstore.tslong.dask import TSLongDask
from tstore.tslong.pandas import TSLongPandas
from tstore.tslong.polars import TSLongPolars
from tstore.tslong.pyarrow import TSLongPyArrow

# Imported fixtures from conftest.py:
# - pandas_long_dataframe
# - polars_long_dataframe


tslong_classes = {
    "dask": TSLongDask,
    "pandas": TSLongPandas,
    "polars": TSLongPolars,
    "pyarrow": TSLongPyArrow,
}


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
    partitioning = {"ts_variable": "year/month"}

    # Each timeseries is a TS object
    ts_variables = ["ts_var1", "ts_var2", "ts_var3", "ts_var4"]
    # Group multiple timeseries into one TS object
    ts_variables = {"ts_variable": ts_variables}

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


@pytest.mark.parametrize("backend", ["pandas", "polars"])
def test_creation(
    backend: Backend,
    request,
) -> None:
    """Test the creation of a TSLong object."""
    dataframe_fixture_name = f"{backend}_long_dataframe"
    df = request.getfixturevalue(dataframe_fixture_name)
    tslong = tstore.TSLong.wrap(df)
    assert isinstance(tslong, tslong_classes[backend])
    assert tslong.shape == df.shape


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
    assert sorted(os.listdir(dirpath)) == ["_attributes.parquet"] + [f"store_id={i}" for i in ["1", "2", "3", "4"]] + [
        "tstore_metadata.yaml",
    ]
    assert os.listdir(dirpath / "store_id=1" / "variable=ts_variable" / "year=2000" / "month=1") == ["part-0.parquet"]


class TestLoad:
    """Test the from_tstore function."""

    def test_pandas(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a Pandas DataFrame."""
        tslong = tstore.open_tslong(tstore_path, backend="pandas", ts_variables=["ts_variable"])
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
        tslong = tstore.open_tslong(tstore_path, backend="polars", ts_variables=["ts_variable"])
        assert type(tslong) is TSLongPolars
        assert type(tslong._df) is pl.DataFrame
        assert tslong.shape == (192, 7)
        # TODO: dataframe should be wrapped in a TSLong object
        # TODO: time column is counted
        # TODO: line order is not preserved
        # TODO: column order is not preserved

    def test_pyarrow(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a PyArrow Table."""
        tslong = tstore.open_tslong(tstore_path, backend="polars", ts_variables=["ts_variable"])
        assert type(tslong) is TSLongPolars
        assert type(tslong._df) is pl.DataFrame
        assert tslong.shape == (192, 7)


@pytest.mark.parametrize("backend_to", ["dask", "pandas", "polars", "pyarrow"])
@pytest.mark.parametrize("backend_from", ["pandas", "polars"])
def test_change_backend(
    backend_from: Backend,
    backend_to: Backend,
    request,
) -> None:
    """Test the change_backend method."""
    dataframe_fixture_name = f"{backend_from}_long_dataframe"
    df = request.getfixturevalue(dataframe_fixture_name)
    tslong = tstore.TSLong.wrap(df)
    tslong_new = tslong.change_backend(new_backend=backend_to)
    assert isinstance(tslong_new, tslong_classes[backend_to])
