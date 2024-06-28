"""Define possible backends for type hinting."""

from typing import Literal, Optional, TypeVar, Union

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

index_support = {
    "dask": True,
    "pandas": True,
    "polars": False,
    "pyarrow": False,
}

DaskDataFrame = dd.DataFrame
PandasDataFrame = pd.DataFrame
PolarsDataFrame = pl.DataFrame
PyArrowDataFrame = pa.Table
DataFrame = Union[DaskDataFrame, PandasDataFrame, PolarsDataFrame, PyArrowDataFrame]

DaskSeries = dd.Series
PandasSeries = pd.Series
PolarsSeries = pl.Series
PyArrowSeries = pa.Array
Series = Union[DaskSeries, PandasSeries, PolarsSeries, PyArrowSeries]


T = TypeVar("T", DataFrame, Series)


def change_backend(
    obj: T,
    new_backend: Backend,
    index_var: Optional[str] = None,
    **backend_kwargs,
) -> T:
    """Change the backend of a dataframe or a series.

    If index_var is provided, the corresponding column is set as the index if
    the backend supports it. Otherwise, it is converted to a regular column
    (DataFrame) or dropped (Series).
    """
    change_backend_functions = {
        DaskDataFrame: _change_dataframe_backend_from_dask,
        PandasDataFrame: _change_dataframe_backend_from_pandas,
        PolarsDataFrame: _change_dataframe_backend_from_polars,
        PyArrowDataFrame: _change_dataframe_backend_from_pyarrow,
        DaskSeries: _change_series_backend_from_dask,
        PandasSeries: _change_series_backend_from_pandas,
        PolarsSeries: _change_series_backend_from_polars,
        PyArrowSeries: _change_series_backend_from_pyarrow,
    }

    for supported_type, change_backend_function in change_backend_functions.items():
        if isinstance(obj, supported_type):
            return change_backend_function(obj, new_backend, index_var=index_var, **backend_kwargs)

    raise TypeError(f"Unsupported type: {type(obj).__module__}.{type(obj).__qualname__}")


def _change_dataframe_backend_from_dask(
    df: DaskDataFrame,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> DataFrame:
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


def _change_dataframe_backend_from_pandas(
    df: PandasDataFrame,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> DataFrame:
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


def _change_dataframe_backend_from_polars(
    df: PolarsDataFrame,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> DataFrame:
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


def _change_dataframe_backend_from_pyarrow(
    df: PyArrowDataFrame,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> DataFrame:
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


def _change_series_backend_from_dask(
    series: dd.Series,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> Series:
    """Change the backend of a Dask series."""
    if new_backend == "dask":
        return series

    if new_backend == "pandas":
        return series.compute(**backend_kwargs)

    if new_backend == "polars":
        return pl.Series(series.compute(), **backend_kwargs)

    if new_backend == "pyarrow":
        return pa.Array.from_pandas(series.compute(), **backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_series_backend_from_pandas(
    series: pd.Series,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> Series:
    """Change the backend of a Pandas series."""
    if new_backend == "dask":
        return dd.from_pandas(series, **backend_kwargs)

    if new_backend == "pandas":
        return series

    if new_backend == "polars":
        return pl.from_pandas(series, **backend_kwargs)

    if new_backend == "pyarrow":
        return pa.Array.from_pandas(series)

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_series_backend_from_polars(
    series: pl.Series,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> Series:
    """Change the backend of a Polars series."""
    if new_backend == "dask":
        return dd.from_pandas(series.to_pandas(use_pyarrow_extension_array=True), **backend_kwargs)

    if new_backend == "pandas":
        backend_kwargs.setdefault("use_pyarrow_extension_array", True)
        return series.to_pandas(**backend_kwargs)

    if new_backend == "polars":
        return series

    if new_backend == "pyarrow":
        return series.to_arrow(**backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


def _change_series_backend_from_pyarrow(
    series: pa.Array,
    new_backend: Backend,
    index_var: Optional[str] = None,  # noqa: ARG001
    **backend_kwargs,
) -> Series:
    """Change the backend of a Pyarrow series."""
    pandas_series = pd.Series(series.to_pandas())

    if new_backend == "dask":
        return dd.from_pandas(pandas_series, **backend_kwargs)

    if new_backend == "pandas":
        return pandas_series

    if new_backend == "polars":
        return pl.Series(pandas_series)

    if new_backend == "pyarrow":
        return series

    raise ValueError(f"Unsupported backend: {new_backend}")
