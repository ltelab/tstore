"""Test the tsdf subpackage."""

import os

import numpy as np
import pandas as pd

import tstore

# Imported fixtures from conftest.py:
# - pandas_tsarray
# - pandas_series_of_ts
# - pandas_tsdf


def test_tsarray_creation(pandas_tsarray: tstore.TSArray) -> None:
    """Test the TSArray wrapper."""
    assert isinstance(pandas_tsarray, tstore.TSArray)
    assert pandas_tsarray.shape == (4,)
    assert isinstance(pandas_tsarray[0], tstore.TS)


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


def test_pandas_tsdf_creation(pandas_tsdf: tstore.TSDF) -> None:
    """Test the TSDF wrapper."""
    assert isinstance(pandas_tsdf, tstore.TSDF)
    assert isinstance(pandas_tsdf["ts_variable"], pd.Series)
    np.testing.assert_array_equal(pandas_tsdf["tstore_id"], ["1", "2", "3", "4"])
    np.testing.assert_array_equal(pandas_tsdf["attribute_1"], ["A", "B", "C", "D"])
    np.testing.assert_array_equal(pandas_tsdf["attribute_2"], [1.0, 2.0, 3.0, 4.0])


def test_add(
    pandas_series_of_ts: pd.Series,
    pandas_tsdf: tstore.TSDF,
) -> None:
    """Test adding a Pandas Series to a TSDF."""
    pandas_tsdf["new_variable"] = pandas_series_of_ts
    assert "new_variable" in pandas_tsdf.columns
    assert isinstance(pandas_tsdf, tstore.TSDF)
    assert isinstance(pandas_tsdf["new_variable"], pd.Series)


def test_drop(pandas_tsdf: tstore.TSDF) -> None:
    """Test dropping a variable."""
    pandas_tsdf = pandas_tsdf.drop(columns=["ts_variable"])
    assert "ts_variable" not in pandas_tsdf.columns
    assert isinstance(pandas_tsdf, tstore.TSDF)


def test_iloc(pandas_tsdf: tstore.TSDF) -> None:
    """Test subsetting a Pandas TSDF with iloc."""
    tsdf = pandas_tsdf.iloc[0:10]
    assert isinstance(tsdf, tstore.TSDF)


def test_store(
    pandas_tsdf: tstore.TSDF,
    tmp_path,
) -> None:
    """Test the to_store method."""
    dirpath = tmp_path / "test_tstore"
    partitioning = None
    tstore_structure = "id-var"
    overwrite = True
    pandas_tsdf.to_tstore(
        str(dirpath),
        id_var="tstore_id",
        time_var="time",
        partitioning=partitioning,
        tstore_structure=tstore_structure,
        overwrite=overwrite,
    )

    # Check that the tstore is created
    assert dirpath.is_dir()

    # Check directory content
    assert sorted(os.listdir(dirpath / "1" / "ts_variable")) == [
        "_common_metadata",
        "_metadata",
        "part.0.parquet",
        "part.1.parquet",
    ]
    assert sorted(os.listdir(dirpath)) == ["1", "2", "3", "4", "_attributes.parquet", "tstore_metadata.yaml"]
