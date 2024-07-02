"""Test the tsdf subpackage."""

import os
from pathlib import Path

import numpy as np
import pandas as pd
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
# - dask_tsarray
# - pandas_series_of_ts
# - dask_tsdf


tsdf_classes = {
    "dask": TSDFDask,
    "pandas": TSDFPandas,
    "polars": TSDFPolars,
    "pyarrow": TSDFPyArrow,
}

tslong_classes = {
    "dask": TSLongDask,
    "pandas": TSLongPandas,
    "polars": TSLongPolars,
    "pyarrow": TSLongPyArrow,
}


def test_tsarray_creation(dask_tsarray: tstore.TSArray) -> None:
    """Test the TSArray wrapper."""
    assert isinstance(dask_tsarray, tstore.TSArray)
    assert dask_tsarray.shape == (4,)
    assert isinstance(dask_tsarray[0], tstore.TS)


def test_pandas_series_of_ts_creation(pandas_series_of_ts: pd.Series) -> None:
    """Test the Pandas Series wrapper on a TSArray."""
    assert isinstance(pandas_series_of_ts, pd.Series)
    assert pandas_series_of_ts.shape == (4,)
    assert isinstance(pandas_series_of_ts[1], tstore.TS)
    assert isinstance(pandas_series_of_ts.dtype, tstore.TSDtype)
    assert isinstance(pandas_series_of_ts.values, tstore.TSArray)
    assert pandas_series_of_ts.values is pandas_series_of_ts.array  # zero-copy reference to the data
    assert isinstance(pandas_series_of_ts.to_numpy(), np.ndarray)


def test_pandas_series_concatenation(pandas_series_of_ts: pd.Series) -> None:
    """Test the concatenation of two Pandas Series."""
    df_series = pd.concat((pandas_series_of_ts, pandas_series_of_ts))
    assert isinstance(df_series, pd.Series)
    assert df_series.shape == (8,)
    assert isinstance(df_series.dtype, tstore.TSDtype)


def test_dask_tsdf_creation(dask_tsdf: tstore.TSDF) -> None:
    """Test the TSDF wrapper."""
    assert isinstance(dask_tsdf, tstore.TSDF)
    assert dask_tsdf.current_backend == "dask"
    assert isinstance(dask_tsdf["ts_var1"], pd.Series)
    assert isinstance(dask_tsdf["ts_var2"], pd.Series)
    np.testing.assert_array_equal(dask_tsdf["tstore_id"], ["1", "2", "3", "4"])
    np.testing.assert_array_equal(dask_tsdf["static_var1"], ["A", "B", "C", "D"])
    np.testing.assert_array_equal(dask_tsdf["static_var2"], [1.0, 2.0, 3.0, 4.0])


def test_attributes(dask_tsdf: tstore.TSDF) -> None:
    """Test the given and computed _tstore_ attributes."""
    assert dask_tsdf._tstore_id_var == "tstore_id"
    assert dask_tsdf._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert dask_tsdf._tstore_static_vars == ["static_var1", "static_var2"]


def test_add(
    pandas_series_of_ts: pd.Series,
    dask_tsdf: tstore.TSDF,
) -> None:
    """Test adding a Pandas Series to a TSDF."""
    dask_tsdf["new_variable"] = pandas_series_of_ts
    assert "new_variable" in dask_tsdf.columns
    assert isinstance(dask_tsdf, tstore.TSDF)
    assert isinstance(dask_tsdf["new_variable"], pd.Series)


def test_drop(dask_tsdf: tstore.TSDF) -> None:
    """Test dropping a variable."""
    dask_tsdf = dask_tsdf.drop(columns=["ts_var1"])
    assert "ts_var1" not in dask_tsdf.columns
    assert isinstance(dask_tsdf, tstore.TSDF)


def test_iloc(dask_tsdf: tstore.TSDF) -> None:
    """Test subsetting a Pandas TSDF with iloc."""
    tsdf = dask_tsdf.iloc[0:10]
    assert isinstance(tsdf, tstore.TSDF)


def test_store(
    dask_tsdf: tstore.TSDF,
    tmp_path,
) -> None:
    """Test the to_store method."""
    dirpath = tmp_path / "test_tstore"
    partitioning = None
    tstore_structure = "id-var"
    overwrite = True
    dask_tsdf.to_tstore(
        str(dirpath),
        partitioning=partitioning,
        tstore_structure=tstore_structure,
        overwrite=overwrite,
    )

    # Check that the tstore is created
    assert dirpath.is_dir()

    # Check directory content
    for ts_var in ["ts_var1", "ts_var2"]:
        assert sorted(os.listdir(dirpath / "1" / ts_var)) == [
            "_common_metadata",
            "_metadata",
            "part.0.parquet",
        ]
    assert sorted(os.listdir(dirpath)) == ["1", "2", "3", "4", "_attributes.parquet", "tstore_metadata.yaml"]


class TestLoad:
    """Test the from_tstore function."""

    def test_dask(
        self,
        tstore_path: Path,
    ) -> None:
        """Test loading as a Dask TSDF."""
        tsdf = tstore.open_tsdf(tstore_path, backend="dask")
        assert type(tsdf) is TSDFDask
        assert type(tsdf._obj) is pd.DataFrame
        assert tsdf._tstore_id_var == "tstore_id"
        assert tsdf._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
        assert tsdf._tstore_static_vars == ["static_var1", "static_var2"]
        assert isinstance(tsdf["ts_var1"], pd.Series)
        assert isinstance(tsdf["ts_var2"], pd.Series)
        np.testing.assert_array_equal(tsdf["tstore_id"], ["1", "2", "3", "4"])
        np.testing.assert_array_equal(tsdf["static_var1"], ["A", "B", "C", "D"])
        np.testing.assert_array_equal(tsdf["static_var2"], [1.0, 2.0, 3.0, 4.0])


@pytest.mark.parametrize("new_backend", ["pandas", "polars", "pyarrow"])
def test_change_backend(
    new_backend: Backend,
    dask_tsdf: tstore.TSDF,
) -> None:
    """Test changing the backend of a TSDF."""
    assert isinstance(dask_tsdf, TSDFDask)
    assert dask_tsdf.current_backend == "dask"

    tsdf_new = dask_tsdf.change_backend(new_backend)
    assert isinstance(tsdf_new, tsdf_classes[new_backend])
    assert tsdf_new.current_backend == new_backend
    assert isinstance(tsdf_new["ts_var1"], pd.Series)
    assert isinstance(tsdf_new["ts_var2"], pd.Series)
    np.testing.assert_array_equal(tsdf_new["tstore_id"], ["1", "2", "3", "4"])
    np.testing.assert_array_equal(tsdf_new["static_var1"], ["A", "B", "C", "D"])
    np.testing.assert_array_equal(tsdf_new["static_var2"], [1.0, 2.0, 3.0, 4.0])


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
def test_to_tslong(
    backend: Backend,
    dask_tsdf: tstore.TSDF,
) -> None:
    """Test the to_tslong function."""
    tsdf = dask_tsdf.change_backend(backend)
    tslong = tsdf.to_tslong()

    assert isinstance(tslong, tslong_classes[backend])
    assert tslong.current_backend == backend
    assert tslong._tstore_id_var == "tstore_id"
    assert tslong._tstore_time_var == "time"
    assert tslong._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tslong._tstore_static_vars == ["static_var1", "static_var2"]
