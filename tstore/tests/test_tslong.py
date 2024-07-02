"""Test the tslong subpackage."""

import os
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import tstore
from tstore.backend import Backend
from tstore.tsdf.dask import TSDFDask
from tstore.tsdf.pandas import TSDFPandas
from tstore.tsdf.polars import TSDFPolars
from tstore.tsdf.pyarrow import TSDFPyArrow
from tstore.tslong.dask import TSLongDask
from tstore.tslong.pandas import TSLongPandas
from tstore.tslong.polars import TSLongPolars
from tstore.tslong.pyarrow import TSLongPyArrow

# Imported fixtures from conftest.py:
# - dask_long_dataframe
# - dask_tslong
# - pandas_long_dataframe
# - pandas_tslong
# - polars_long_dataframe
# - polars_tslong
# - pyarrow_long_dataframe
# - pyarrow_tslong


tslong_classes = {
    "dask": TSLongDask,
    "pandas": TSLongPandas,
    "polars": TSLongPolars,
    "pyarrow": TSLongPyArrow,
}

tsdf_classes = {
    "dask": TSDFDask,
    "pandas": TSDFPandas,
    "polars": TSDFPolars,
    "pyarrow": TSDFPyArrow,
}


# Functions ####################################################################


def store_tslong(tslong: tstore.TSLong, dirpath: str) -> None:
    tstore_structure = "id-var"
    overwrite = True
    # geometry = None  # NOT IMPLEMENTED YET

    # Same partitioning for all TS
    partitioning = "year/month"
    # Partitioning specific to each TS
    partitioning = {"ts_var1": "year/month", "ts_var2": "year/month"}

    tslong.to_tstore(
        # TSTORE options
        dirpath,
        # TSTORE options
        partitioning=partitioning,
        tstore_structure=tstore_structure,
        overwrite=overwrite,
    )


# Tests ########################################################################


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
def test_creation(
    backend: Backend,
    request,
) -> None:
    """Test the creation of a TSLong object."""
    dataframe_fixture_name = f"{backend}_long_dataframe"
    df = request.getfixturevalue(dataframe_fixture_name)
    tslong = tstore.TSLong.wrap(
        df,
        id_var="tstore_id",
        time_var="time",
        ts_vars={"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]},
        static_vars=["static_var1", "static_var2"],
    )

    assert isinstance(tslong, tslong_classes[backend])
    assert tslong.current_backend == backend

    if isinstance(tslong, TSLongDask):
        assert tslong._obj.compute().shape == df.compute().shape
    else:
        assert tslong.shape == df.shape

    assert tslong._tstore_id_var == "tstore_id"
    assert tslong._tstore_time_var == "time"
    assert tslong._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tslong._tstore_static_vars == ["static_var1", "static_var2"]

    if isinstance(tslong, (TSLongDask, TSLongPandas)):
        assert tslong["tstore_id"].dtype == "large_string[pyarrow]"
    elif isinstance(tslong, TSLongPolars):
        assert tslong["tstore_id"].dtype == pl.String
    elif isinstance(tslong, TSLongPyArrow):
        assert tslong["tstore_id"].type == pa.large_string()


@pytest.mark.parametrize(
    "dataframe_fixture_name",
    [
        "dask_tslong",
        "pandas_tslong",
        "polars_tslong",
        "pyarrow_tslong",
    ],
)
def test_store(
    tmp_path: Path,
    dataframe_fixture_name: str,
    request,
) -> None:
    """Test the to_tstore function."""
    tslong = request.getfixturevalue(dataframe_fixture_name)
    dirpath = tmp_path / "test_tstore"
    store_tslong(tslong, str(dirpath))

    # Check that the tstore is created
    assert dirpath.is_dir()

    # Check directory content
    assert sorted(os.listdir(dirpath)) == ["1", "2", "3", "4", "_attributes.parquet", "tstore_metadata.yaml"]
    for ts_var in ["ts_var1", "ts_var2"]:
        assert os.listdir(dirpath / "1" / ts_var / "year=2000" / "month=1") == ["part-0.parquet"]


class TestLoad:
    """Test the from_tstore function."""

    def common_checks(self, tslong: tstore.TSLong) -> None:
        if isinstance(tslong, TSLongDask):
            assert tslong._obj.compute().shape[0] == 192
        else:
            assert tslong.shape[0] == 192
        assert tslong._tstore_id_var == "tstore_id"
        assert tslong._tstore_time_var == "time"
        assert tslong._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
        assert tslong._tstore_static_vars == ["static_var1", "static_var2"]

    def test_dask(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a Dask DataFrame."""
        tslong = tstore.open_tslong(tstore_path, backend="dask", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongDask
        assert type(tslong._obj) is dd.DataFrame
        self.common_checks(tslong)

    def test_pandas(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a Pandas DataFrame."""
        tslong = tstore.open_tslong(tstore_path, backend="pandas", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongPandas
        assert type(tslong._obj) is pd.DataFrame
        self.common_checks(tslong)
        # TODO: time column is counted
        # TODO: line order is not preserved
        # TODO: column order is not preserved

    def test_polars(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a Polars DataFrame."""
        tslong = tstore.open_tslong(tstore_path, backend="polars", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongPolars
        assert type(tslong._obj) is pl.DataFrame
        self.common_checks(tslong)
        # TODO: time column is counted
        # TODO: line order is not preserved
        # TODO: column order is not preserved

    def test_pyarrow(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a PyArrow Table."""
        tslong = tstore.open_tslong(tstore_path, backend="polars", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongPolars
        assert type(tslong._obj) is pl.DataFrame
        self.common_checks(tslong)


@pytest.mark.parametrize("backend_to", ["dask", "pandas", "polars", "pyarrow"])
@pytest.mark.parametrize("backend_from", ["dask", "pandas", "polars", "pyarrow"])
def test_change_backend(
    backend_from: Backend,
    backend_to: Backend,
    request,
) -> None:
    """Test the change_backend method."""
    tslong_fixture_name = f"{backend_from}_tslong"
    tslong = request.getfixturevalue(tslong_fixture_name)
    tslong_new = tslong.change_backend(new_backend=backend_to)
    assert isinstance(tslong_new, tslong_classes[backend_to])


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
def test_to_tsdf(
    backend: str,
    request,
) -> None:
    """Test the to_tsdf function."""
    tslong_fixture_name = f"{backend}_tslong"
    tslong = request.getfixturevalue(tslong_fixture_name)
    tsdf = tslong.to_tsdf()

    assert isinstance(tsdf, tsdf_classes[backend])
    assert tsdf._tstore_id_var == "tstore_id"
    assert tsdf._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tsdf._tstore_static_vars == ["static_var1", "static_var2"]
    assert isinstance(tsdf["ts_var1"], pd.Series)
    assert isinstance(tsdf["ts_var2"], pd.Series)
    np.testing.assert_array_equal(tsdf["tstore_id"], ["1", "2", "3", "4"])
    np.testing.assert_array_equal(tsdf["static_var1"], ["A", "B", "C", "D"])
    np.testing.assert_array_equal(tsdf["static_var2"], [1.0, 2.0, 3.0, 4.0])
