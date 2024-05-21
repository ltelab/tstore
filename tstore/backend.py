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


def change_backend(df: DataFrame, new_backend: Backend, **backend_kwargs) -> DataFrame:
    """Change the backend of a dataframe."""
    if isinstance(df, DaskDataFrame):
        return _change_backend_from_dask(df, new_backend, **backend_kwargs)

    if isinstance(df, PandasDataFrame):
        return _change_backend_from_pandas(df, new_backend, **backend_kwargs)

    if isinstance(df, PolarsDataFrame):
        return _change_backend_from_polars(df, new_backend, **backend_kwargs)

    if isinstance(df, PyArrowDataFrame):
        return _change_backend_from_pyarrow(df, new_backend, **backend_kwargs)

    raise TypeError(f"Unsupported dataframe type: {type(df).__module__}.{type(df).__qualname__}")


def _change_backend_from_dask(df: DaskDataFrame, new_backend: Backend, **backend_kwargs) -> DataFrame:
    """Change the backend of a Dask dataframe."""
    if new_backend == "dask":
        return df

    if new_backend == "pandas":
        return df.compute(**backend_kwargs)

    if new_backend == "polars":
        return pl.from_pandas(df.compute(), **backend_kwargs)

    if new_backend == "pyarrow":
        return pa.Table.from_pandas(df.compute(), **backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_backend_from_pandas(df: PandasDataFrame, new_backend: Backend, **backend_kwargs) -> DataFrame:
    """Change the backend of a Pandas dataframe."""
    if new_backend == "dask":
        return dd.from_pandas(df, **backend_kwargs)

    if new_backend == "pandas":
        return df

    if new_backend == "polars":
        return pl.from_pandas(df, **backend_kwargs)

    if new_backend == "pyarrow":
        return pa.Table.from_pandas(df, **backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_backend_from_polars(df: PolarsDataFrame, new_backend: Backend, **backend_kwargs) -> DataFrame:
    """Change the backend of a Polars dataframe."""
    if new_backend == "dask":
        return dd.from_pandas(df.to_pandas(use_pyarrow_extension_array=True), **backend_kwargs)

    if new_backend == "pandas":
        backend_kwargs.setdefault("use_pyarrow_extension_array", True)
        return df.to_pandas(**backend_kwargs)

    if new_backend == "polars":
        return df

    if new_backend == "pyarrow":
        return df.to_arrow(**backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_backend_from_pyarrow(df: PyArrowDataFrame, new_backend: Backend, **backend_kwargs) -> DataFrame:
    """Change the backend of a PyArrow table."""
    if new_backend == "dask":
        return dd.from_pandas(df.to_pandas(types_mapper=pd.ArrowDtype), **backend_kwargs)

    if new_backend == "pandas":
        backend_kwargs.setdefault("types_mapper", pd.ArrowDtype)
        return df.to_pandas(**backend_kwargs)

    if new_backend == "polars":
        return pl.from_arrow(df, **backend_kwargs)

    if new_backend == "pyarrow":
        return df

    raise ValueError(f"Unsupported backend: {new_backend}")
