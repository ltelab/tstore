"""Define possible backends for type hinting."""

from typing import Literal, Union

import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow as pa

Backend = Literal[
    "dask",
    "pandas",
    "polars",
    "pyarrow",
]

DaskDataFrame = dd.DataFrame
PandasDataFrame = pd.DataFrame
PolarsDataFrame = pl.DataFrame
PyArrowDataFrame = pa.Table
DataFrame = Union[DaskDataFrame, PandasDataFrame, PolarsDataFrame, PyArrowDataFrame]


def change_backend(df: DataFrame, new_backend: Backend) -> DataFrame:
    """Change the backend of a dataframe."""
    if isinstance(df, DaskDataFrame):
        return _change_backend_from_dask(df, new_backend)

    if isinstance(df, PandasDataFrame):
        return _change_backend_from_pandas(df, new_backend)

    if isinstance(df, PolarsDataFrame):
        return _change_backend_from_polars(df, new_backend)

    if isinstance(df, PyArrowDataFrame):
        return _change_backend_from_pyarrow(df, new_backend)

    raise TypeError(f"Unsupported dataframe type: {type(df).__module__}.{type(df).__qualname__}")


def _change_backend_from_dask(df: DaskDataFrame, new_backend: Backend) -> DataFrame:
    """Change the backend of a Dask dataframe."""
    if new_backend == "dask":
        return df

    if new_backend == "pandas":
        return df.compute()

    if new_backend == "polars":
        return pl.DataFrame(df.compute())

    if new_backend == "pyarrow":
        return pa.Table.from_pandas(df.compute())

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_backend_from_pandas(df: PandasDataFrame, new_backend: Backend) -> DataFrame:
    """Change the backend of a Pandas dataframe."""
    if new_backend == "dask":
        return dd.from_pandas(df, npartitions=1)

    if new_backend == "pandas":
        return df

    if new_backend == "polars":
        return pl.DataFrame(df)

    if new_backend == "pyarrow":
        return pa.Table.from_pandas(df)

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_backend_from_polars(df: PolarsDataFrame, new_backend: Backend) -> DataFrame:
    """Change the backend of a Polars dataframe."""
    if new_backend == "dask":
        return dd.from_pandas(df.to_pandas(), npartitions=1)

    if new_backend == "pandas":
        return df.to_pandas()

    if new_backend == "polars":
        return df

    if new_backend == "pyarrow":
        return df.to_arrow()

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_backend_from_pyarrow(df: PyArrowDataFrame, new_backend: Backend) -> DataFrame:
    """Change the backend of a PyArrow table."""
    if new_backend == "dask":
        return dd.from_pandas(df.to_pandas(), npartitions=1)

    if new_backend == "pandas":
        return df.to_pandas()

    if new_backend == "polars":
        return pl.DataFrame(df.to_pandas())

    if new_backend == "pyarrow":
        return df

    raise ValueError(f"Unsupported backend: {new_backend}")
