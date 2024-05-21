"""Test the backend management package."""

import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from tstore import backend

# Fixtures imported from conftest.py:
# - dask_dataframe
# - pandas_dataframe
# - polars_dataframe
# - pyarrow_dataframe


dataframe_types = {
    "dask": dd.DataFrame,
    "pandas": pd.DataFrame,
    "polars": pl.DataFrame,
    "pyarrow": pa.Table,
}

backend_names = list(dataframe_types.keys())


def test_types() -> None:
    """Test the types defined in the backend module."""
    assert backend.DaskDataFrame is dd.DataFrame
    assert backend.PandasDataFrame is pd.DataFrame
    assert backend.PolarsDataFrame is pl.DataFrame
    assert backend.PyArrowDataFrame is pa.Table


@pytest.mark.parametrize("backend_to", backend_names)
@pytest.mark.parametrize("backend_from", backend_names)
def test_change_backend(
    backend_from: backend.Backend,
    backend_to: backend.Backend,
    request,
) -> None:
    """Test the change_backend function."""
    dataframe_fixture_name = f"{backend_from}_dataframe"
    df = request.getfixturevalue(dataframe_fixture_name)
    assert isinstance(df, dataframe_types[backend_from])

    df_new = backend.change_backend(df=df, new_backend=backend_to)
    assert isinstance(df_new, dataframe_types[backend_to])

    if backend_from == "dask":
        df = df.compute()

    if backend_to == "dask":
        df_new = df_new.compute()

    # Check size
    assert df.shape[0] == df_new.shape[0]


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
    df_temp = backend.change_backend(df=df, new_backend=backend_to)
    df_new = backend.change_backend(df=df_temp, new_backend=backend_from)

    if backend_from == "dask":
        df = df.compute()
        df_new = df_new.compute()

    # Check shape
    assert df.shape == df_new.shape

    # Check values
    # TODO: Polars and PyArrow don't keep the index column
    assert df.equals(df_new)
