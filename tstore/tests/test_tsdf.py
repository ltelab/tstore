"""Test the tsdf subpackage."""

import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

import tstore
from tstore.backend import Backend
from tstore.tsdf.geopandas import TSDFGeoPandas
from tstore.tsdf.pandas import TSDFPandas
from tstore.tsdf.tsdf import TSDF
from tstore.tslong.dask import TSLongDask
from tstore.tslong.pandas import TSLongPandas
from tstore.tslong.polars import TSLongPolars
from tstore.tslong.pyarrow import TSLongPyArrow

# Imported fixtures from conftest.py:
# - dask_tsarray
# - pandas_series_of_ts
# - tsdf_ts_dask


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


def test_tsdf_creation(tsdf_ts_dask: tstore.TSDF) -> None:
    """Test the TSDF wrapper."""
    assert isinstance(tsdf_ts_dask, TSDFPandas)
    assert tsdf_ts_dask.get_ts_backend("ts_var1") == "dask"
    assert isinstance(tsdf_ts_dask["ts_var1"], pd.Series)
    assert isinstance(tsdf_ts_dask["ts_var2"], pd.Series)
    np.testing.assert_array_equal(tsdf_ts_dask["tstore_id"], ["1", "2", "3", "4"])
    np.testing.assert_array_equal(tsdf_ts_dask["static_var1"], ["A", "B", "C", "D"])
    np.testing.assert_array_equal(tsdf_ts_dask["static_var2"], [1.0, 2.0, 3.0, 4.0])


def test_geo_tsdf_creation(geo_tsdf_ts_dask: tstore.TSDF) -> None:
    """Test the TSDF wrapper on a GeoPandas dataframe."""
    assert isinstance(geo_tsdf_ts_dask, TSDFGeoPandas)
    assert geo_tsdf_ts_dask.get_ts_backend("ts_var1") == "dask"
    assert isinstance(geo_tsdf_ts_dask["ts_var1"], pd.Series)
    assert isinstance(geo_tsdf_ts_dask["ts_var2"], pd.Series)
    assert isinstance(geo_tsdf_ts_dask["geometry"], gpd.GeoSeries)


def test_attributes(tsdf_ts_dask: tstore.TSDF) -> None:
    """Test the given and computed _tstore_ attributes."""
    assert tsdf_ts_dask._tstore_id_var == "tstore_id"
    assert tsdf_ts_dask._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tsdf_ts_dask._tstore_static_vars == ["static_var1", "static_var2"]


def test_add(
    pandas_series_of_ts: pd.Series,
    tsdf_ts_dask: tstore.TSDF,
) -> None:
    """Test adding a Pandas Series to a TSDF."""
    tsdf_ts_dask["new_variable"] = pandas_series_of_ts
    assert "new_variable" in tsdf_ts_dask.columns
    assert isinstance(tsdf_ts_dask, tstore.TSDF)
    assert isinstance(tsdf_ts_dask["new_variable"], pd.Series)


def test_drop(tsdf_ts_dask: tstore.TSDF) -> None:
    """Test dropping a variable."""
    tsdf_ts_dask = tsdf_ts_dask.drop(columns=["ts_var1"])
    assert "ts_var1" not in tsdf_ts_dask.columns
    assert isinstance(tsdf_ts_dask, tstore.TSDF)


def test_iloc(tsdf_ts_dask: tstore.TSDF) -> None:
    """Test subsetting a Pandas TSDF with iloc."""
    tsdf = tsdf_ts_dask.iloc[0:10]
    assert isinstance(tsdf, tstore.TSDF)


def test_store(
    tsdf_ts_dask: tstore.TSDF,
    tmp_path,
) -> None:
    """Test the to_store method."""
    dirpath = tmp_path / "test_tstore"
    partitioning = None
    tstore_structure = "id-var"
    overwrite = True
    tsdf_ts_dask.to_tstore(
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
        assert type(tsdf) is TSDFPandas
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
    tsdf_ts_dask: tstore.TSDF,
) -> None:
    """Test changing the backend of a TSDF."""
    assert isinstance(tsdf_ts_dask, TSDF)
    assert tsdf_ts_dask.get_ts_backend("ts_var1") == "dask"
    assert tsdf_ts_dask.get_ts_backend("ts_var2") == "dask"

    # Change one of the TS variables
    tsdf_new = tsdf_ts_dask.change_ts_backend(new_backend, ts_cols=["ts_var1"])
    assert tsdf_new.get_ts_backend("ts_var1") == new_backend
    assert tsdf_new.get_ts_backend("ts_var2") == "dask"
    assert isinstance(tsdf_new["ts_var1"], pd.Series)
    assert isinstance(tsdf_new["ts_var2"], pd.Series)
    np.testing.assert_array_equal(tsdf_new["tstore_id"], ["1", "2", "3", "4"])
    np.testing.assert_array_equal(tsdf_new["static_var1"], ["A", "B", "C", "D"])
    np.testing.assert_array_equal(tsdf_new["static_var2"], [1.0, 2.0, 3.0, 4.0])

    # Change all TS variables
    tsdf_new = tsdf_ts_dask.change_ts_backend(new_backend)
    assert tsdf_new.get_ts_backend("ts_var1") == new_backend
    assert tsdf_new.get_ts_backend("ts_var2") == new_backend


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
def test_to_tslong(
    backend: Backend,
    tsdf_ts_dask: tstore.TSDF,
) -> None:
    """Test the to_tslong function."""
    tslong = tsdf_ts_dask.to_tslong(backend=backend)

    assert isinstance(tslong, tslong_classes[backend])
    assert tslong.current_backend == backend
    assert tsdf_ts_dask.get_ts_backend("ts_var1") == "dask"
    assert tslong._tstore_id_var == "tstore_id"
    assert tslong._tstore_time_var == "time"
    assert tslong._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tslong._tstore_static_vars == ["static_var1", "static_var2"]
