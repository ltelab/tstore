"""Fixtures for the tests."""

from pathlib import Path

import dask
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest


@pytest.fixture()
def dask_dataframe() -> dask.dataframe.DataFrame:
    """Create a Dask DataFrame with a dummy time series.

    The columns are:
        - name: str
        - id: int
        - x: float
        - y: float
    """
    df_dask = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-03",
        freq="1h",
        partition_freq="1d",
        dtypes=None,
        seed=None,
    )

    return df_dask


@pytest.fixture()
def pandas_dataframe(dask_dataframe: dask.dataframe.DataFrame) -> pd.DataFrame:
    """Create a Pandas DataFrame with a dummy time series."""
    df_pd = dask_dataframe.compute()
    return df_pd


@pytest.fixture()
def arrow_dataframe(pandas_dataframe: pd.DataFrame) -> pa.Table:
    """Create an Arrow Table with a dummy time series."""
    df_arrow = pa.Table.from_pandas(pandas_dataframe)
    return df_arrow


@pytest.fixture()
def polars_dataframe(arrow_dataframe: pa.Table) -> pl.DataFrame:
    """Create a Polars DataFrame with a dummy time series."""
    df_pl = pl.from_arrow(arrow_dataframe)
    return df_pl


@pytest.fixture()
def pandas_dataframe_arrow_dtypes(arrow_dataframe: pa.Table) -> pd.DataFrame:
    """Create a Pandas DataFrame with Arrow dtypes."""
    df_pd = arrow_dataframe.to_pandas(types_mapper=pd.ArrowDtype)
    return df_pd


@pytest.fixture()
def parquet_timeseries(tmp_path: Path, dask_dataframe: dask.dataframe.DataFrame) -> Path:
    """Create a Parquet file with a dummy time series."""
    filepath = tmp_path / "test.parquet"

    dask_dataframe.to_parquet(
        str(filepath),
        engine="pyarrow",
        # Index option
        write_index=True,
        # Metadata
        custom_metadata=None,
        write_metadata_file=True,  # enable writing the _metadata file
        # File structure
        name_function=lambda i: f"part.{i}.parquet",  # default naming scheme
        partition_on=None,
        # Encoding
        schema="infer",
        compression="snappy",
        # Writing options
        append=False,
        overwrite=False,
        ignore_divisions=False,
        compute=True,
    )

    return filepath
