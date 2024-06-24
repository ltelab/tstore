"""Fixtures for the tests."""

from pathlib import Path
from typing import Optional

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import tstore

# Constants


ID_VAR = "tstore_id"
TIME_VAR = "time"
TS_VAR1 = "ts_var1"
TS_VAR2 = "ts_var2"
TS_VARS = {TS_VAR1: ["var1", "var2"], TS_VAR2: ["var3", "var4"]}
STATIC_VAR1 = "static_var1"
STATIC_VAR2 = "static_var2"
STATIC_VARS = [STATIC_VAR1, STATIC_VAR2]


# Functions


class Helpers:
    """Helper functions for the tests."""

    @staticmethod
    def create_dask_dataframe() -> dd.DataFrame:
        """Create a Dask DataFrame with a dummy time series.

        The columns are:
            - var1: str
            - var2: int
            - var3: float
            - var4: float
        """
        df_dask = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-03",
            freq="1h",
            partition_freq="1d",
            dtypes=None,
            seed=None,
        )

        # TODO: Replace index by regular column
        # df_dask = df_dask.reset_index()
        # df_dask = df_dask.rename(columns={"index": TIME_VAR})

        # Rename columns
        column_names = df_dask.columns
        new_column_names = [f"var{i + 1}" for i in range(len(column_names))]
        df_dask = df_dask.rename(columns=dict(zip(column_names, new_column_names)))

        return df_dask

    @staticmethod
    def create_pandas_tsarray(size: int = 4, columns_slice: Optional[slice] = None) -> tstore.TSArray:
        """Create a TSArray of TS objects."""
        if columns_slice is None:
            columns_slice = slice(0, 4)

        ts_list = []

        for _ in range(size):
            df = Helpers.create_dask_dataframe()
            df = df.iloc[:, columns_slice].compute()
            ts = tstore.TS(df)
            ts_list.append(ts)

        tsarray = tstore.TSArray(ts_list)
        return tsarray


@pytest.fixture()
def helpers() -> type[Helpers]:
    """Return the class of helper functions."""
    return Helpers


# Fixtures

## DataFrames


@pytest.fixture()
def dask_dataframe(helpers) -> dd.DataFrame:
    """Create a Dask DataFrame with a dummy time series."""
    return helpers.create_dask_dataframe()


@pytest.fixture()
def pandas_dataframe(dask_dataframe: dd.DataFrame) -> pd.DataFrame:
    """Create a Pandas DataFrame with a dummy time series."""
    df_pd = dask_dataframe.compute()
    return df_pd


@pytest.fixture()
def pyarrow_dataframe(pandas_dataframe: pd.DataFrame) -> pa.Table:
    """Create an PyArrow Table with a dummy time series."""
    df_pyarrow = pa.Table.from_pandas(pandas_dataframe)
    return df_pyarrow


@pytest.fixture()
def polars_dataframe(pyarrow_dataframe: pa.Table) -> pl.DataFrame:
    """Create a Polars DataFrame with a dummy time series."""
    df_pl = pl.from_arrow(pyarrow_dataframe)
    return df_pl


@pytest.fixture()
def pandas_dataframe_arrow_dtypes(pyarrow_dataframe: pa.Table) -> pd.DataFrame:
    """Create a Pandas DataFrame with Arrow dtypes."""
    df_pd = pyarrow_dataframe.to_pandas(types_mapper=pd.ArrowDtype)
    return df_pd


## Series


@pytest.fixture()
def pandas_series(pandas_dataframe: pd.DataFrame) -> pd.Series:
    """Create a dummy Pandas Series of floats."""
    series = pandas_dataframe["var3"]
    return series


@pytest.fixture()
def dask_series(pandas_series: pd.Series) -> dd.DataFrame:
    """Create a Dask Series from a Pandas Series."""
    series = dd.from_pandas(pandas_series)
    return series


@pytest.fixture()
def polars_series(pandas_series: pd.Series) -> pl.Series:
    """Create a Polars Series from a Pandas Series."""
    series = pl.from_pandas(pandas_series)
    return series


@pytest.fixture()
def pyarrow_series(pandas_series: pd.Series) -> pa.Array:
    """Create a PyArrow Array from a Pandas Series."""
    series = pa.Array.from_pandas(pandas_series)
    return series


## Stored data


@pytest.fixture()
def parquet_timeseries(tmp_path: Path, dask_dataframe: dd.DataFrame) -> Path:
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
    from tstore.tslong.pandas import TSLongPandas

    dirpath = tmp_path / "test_tstore"
    tstore_structure = "id-var"
    overwrite = True
    # geometry = None  # NOT IMPLEMENTED YET

    # Same partitioning for all TS
    partitioning = "year/month"
    # Partitioning specific to each TS
    partitioning = {TS_VAR1: "year/month", TS_VAR2: "year/month"}

    tslong = TSLongPandas(
        pandas_long_dataframe,
        id_var=ID_VAR,
        time_var=TIME_VAR,
        ts_vars=TS_VARS,
        static_vars=STATIC_VARS,
    )

    tslong.to_tstore(
        str(dirpath),
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
        df[ID_VAR] = store_id
        df[STATIC_VAR1] = chr(64 + store_id)  # A, B, C, D
        df[STATIC_VAR2] = float(store_id)  # 1.0, 2.0, 3.0, 4.0
        df_list.append(df)

    df = pd.concat(df_list)
    return df


@pytest.fixture()
def polars_long_dataframe(pandas_long_dataframe: pd.DataFrame) -> pl.DataFrame:
    """Create a long Polars DataFrame."""
    df_pl = pl.from_pandas(pandas_long_dataframe, include_index=True)

    # TODO: Should these be necessary?
    df_pl = df_pl.rename({"timestamp": "time"})
    df_pl = df_pl.with_columns(pl.col(ID_VAR).cast(str))

    return df_pl


@pytest.fixture()
def pandas_tslong(pandas_long_dataframe: pd.DataFrame) -> tstore.TSLong:
    """Create a TSLong object."""
    tslong = tstore.TSLong.wrap(
        pandas_long_dataframe,
        id_var=ID_VAR,
        time_var=TIME_VAR,
        ts_vars=TS_VARS,
        static_vars=STATIC_VARS,
    )
    return tslong


@pytest.fixture()
def polars_tslong(polars_long_dataframe: pl.DataFrame) -> tstore.TSLong:
    """Create a TSLong object."""
    tslong = tstore.TSLong.wrap(
        polars_long_dataframe,
        id_var=ID_VAR,
        time_var=TIME_VAR,
        ts_vars=TS_VARS,
        static_vars=STATIC_VARS,
    )
    return tslong


## TSArrays


@pytest.fixture()
def pandas_tsarray(helpers) -> tstore.TSArray:
    """Create a TSArray of TS objects."""
    return helpers.create_pandas_tsarray(size=4)


## Pandas Series


@pytest.fixture()
def pandas_series_of_ts(pandas_tsarray: tstore.TSArray) -> pd.Series:
    """Create a Pandas Series of TS objects from a TSArray."""
    df_series = pd.Series(pandas_tsarray)
    return df_series


## TSDF


@pytest.fixture()
def pandas_tsdf(helpers) -> tstore.TSDF:
    """Create a TSDF object."""
    pd_series_of_ts_1 = pd.Series(helpers.create_pandas_tsarray(size=4, columns_slice=slice(0, 2)))
    pd_series_of_ts_2 = pd.Series(helpers.create_pandas_tsarray(size=4, columns_slice=slice(2, 4)))
    tstore_ids = np.arange(1, pd_series_of_ts_1.size + 1)
    data = {
        ID_VAR: tstore_ids,
        TS_VAR1: pd_series_of_ts_1,
        TS_VAR2: pd_series_of_ts_2,
        STATIC_VAR1: ["A", "B", "C", "D"],
        STATIC_VAR2: [1.0, 2.0, 3.0, 4.0],
    }

    df = pd.DataFrame(data)
    df[ID_VAR] = df[ID_VAR].astype("large_string[pyarrow]")

    tsdf = tstore.TSDF.wrap(
        df,
        id_var=ID_VAR,
        time_var=TIME_VAR,
    )
    return tsdf
