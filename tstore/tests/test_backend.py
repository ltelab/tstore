"""Test the backend management package."""

import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from tstore import backend

# Fixtures imported from conftest.py:
# - dask_dataframe
# - dask_series
# - pandas_dataframe
# - pandas_series
# - polars_dataframe
# - polars_series
# - pyarrow_dataframe
# - pyarrow_series


dataframe_types = {
    "dask": dd.DataFrame,
    "pandas": pd.DataFrame,
    "polars": pl.DataFrame,
    "pyarrow": pa.Table,
}

series_types = {
    "dask": dd.Series,
    "pandas": pd.Series,
    "polars": pl.Series,
    "pyarrow": pa.Array,
}

backend_names = list(dataframe_types.keys())


def test_types() -> None:
    """Test the types defined in the backend module."""
    assert backend.DaskDataFrame is dd.DataFrame
    assert backend.DaskSeries is dd.Series
    assert backend.PandasDataFrame is pd.DataFrame
    assert backend.PandasSeries is pd.Series
    assert backend.PolarsDataFrame is pl.DataFrame
    assert backend.PolarsSeries is pl.Series
    assert backend.PyArrowDataFrame is pa.Table
    assert backend.PyArrowSeries is pa.Array


@pytest.mark.parametrize("backend_to", backend_names)
@pytest.mark.parametrize("backend_from", backend_names)
def test_change_dataframe_backend(
    backend_from: backend.Backend,
    backend_to: backend.Backend,
    request,
) -> None:
    """Test the change_backend function on dataframes."""
    dataframe_fixture_name = f"{backend_from}_dataframe"
    df = request.getfixturevalue(dataframe_fixture_name)
    assert isinstance(df, dataframe_types[backend_from])

    df_new = backend.change_backend(obj=df, new_backend=backend_to)
    assert isinstance(df_new, dataframe_types[backend_to])

    if backend_from == "dask":
        df = df.compute()

    if backend_to == "dask":
        df_new = df_new.compute()

    # Check size
    assert df.shape[0] == df_new.shape[0]
    print(backend_from, df.shape, backend_to, df_new.shape)


@pytest.mark.parametrize("backend_to", backend_names)
@pytest.mark.parametrize("backend_from", backend_names)
def test_change_series_backend(
    backend_from: backend.Backend,
    backend_to: backend.Backend,
    request,
) -> None:
    """Test the change_backend function on series."""
    series_fixture_name = f"{backend_from}_series"
    series = request.getfixturevalue(series_fixture_name)
    assert isinstance(series, series_types[backend_from])

    series_new = backend.change_backend(obj=series, new_backend=backend_to)
    assert isinstance(series_new, series_types[backend_to])

    # Check size
    assert len(series) == len(series_new)


@pytest.mark.parametrize("backend_to", backend_names)
@pytest.mark.parametrize("backend_from", backend_names)
def test_change_backend_to_and_fro(
    backend_from: backend.Backend,
    backend_to: backend.Backend,
    request,
) -> None:
    """Test the change_backend function with A -> B -> A and check for equality."""
    dataframe_fixture_name = f"{backend_from}_dataframe"
    df = request.getfixturevalue(dataframe_fixture_name)
    df_temp = backend.change_backend(obj=df, new_backend=backend_to)
    df_new = backend.change_backend(obj=df_temp, new_backend=backend_from)

    if backend_from == "dask":
        df = df.compute()
        df_new = df_new.compute()

    # Check shape
    assert df.shape == df_new.shape

    # Check values
    # TODO: Polars and PyArrow don't keep the index column
    assert df.equals(df_new)


def test_polars_to_pandas(polars_dataframe: pl.DataFrame) -> None:
    """Test the .to_pandas() method on a Polars DataFrame with PyArrow dtype using different kwargs."""
    # Check that both kwargs return the same DataFrame
    df1 = polars_dataframe.to_pandas(use_pyarrow_extension_array=True)
    df2 = polars_dataframe.to_pandas(types_mapper=pd.ArrowDtype)
    pd.testing.assert_frame_equal(df1, df2)

    # Check that dtypes are ArrowDtypes
    for df in [df1, df2]:
        for dtype in df.dtypes:
            assert isinstance(dtype, pd.ArrowDtype)
