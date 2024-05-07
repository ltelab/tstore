"""Fixtures for the tests."""

from pathlib import Path

import dask
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

# Functions


class Helpers:
    """Helper functions for the tests."""

    @staticmethod
    def create_dask_dataframe():
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
def helpers() -> type[Helpers]:
    """Return the class of helper functions."""
    return Helpers


# Fixtures

## DataFrames


@pytest.fixture()
def dask_dataframe(helpers) -> dask.dataframe.DataFrame:
    """Create a Dask DataFrame with a dummy time series."""
    return helpers.create_dask_dataframe()


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


## Stored data


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


@pytest.fixture()
def tstore_path(tmp_path: Path, pandas_long_dataframe: pd.DataFrame) -> Path:
    """Store a Pandas long DataFrame as a TStore."""
    # TODO: Rewrite without using tstore to not depend on implementation
    from tstore.tslong.pandas import to_tstore

    dirpath = tmp_path / "test_tstore"
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

    to_tstore(
        pandas_long_dataframe,
        # TSTORE options
        str(dirpath),
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

    return dirpath


## Long DataFrames


@pytest.fixture()
def pandas_long_dataframe(helpers) -> pd.DataFrame:
    """Create a long Pandas DataFrame."""
    store_ids = np.arange(1, 4 + 1)

    df_list = []

    for store_id in store_ids:
        df = helpers.create_dask_dataframe().compute()
        df["store_id"] = store_id
        df["static_var"] = f"{store_id}_static"
        df_list.append(df)

    df = pd.concat(df_list)
    df["static_var"] = df["store_id"].astype(str) + "_static"
    return df


@pytest.fixture()
def polars_long_dataframe(pandas_long_dataframe: pd.DataFrame) -> pl.DataFrame:
    """Create a long Polars DataFrame."""
    df_pl = pl.from_pandas(pandas_long_dataframe, include_index=True)

    # TODO: Should these be necessary?
    df_pl = df_pl.rename({"timestamp": "time"})
    df_pl = df_pl.with_columns(pl.col("store_id").cast(str))

    return df_pl
