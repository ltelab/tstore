"""Define possible backends for type hinting."""

from typing import Callable, Literal, Optional, TypeVar, Union

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

DaskSeries = dd.Series
PandasSeries = pd.Series
PolarsSeries = pl.Series
PyArrowSeries = pa.Array
Series = Union[DaskSeries, PandasSeries, PolarsSeries, PyArrowSeries]


T = TypeVar("T", DataFrame, Series)


def get_backend(obj: T) -> Backend:
    """Get the backend of a dataframe or a series."""
    if isinstance(obj, (DaskDataFrame, DaskSeries)):
        return "dask"

    if isinstance(obj, (PandasDataFrame, PandasSeries)):
        return "pandas"

    if isinstance(obj, (PolarsDataFrame, PolarsSeries)):
        return "polars"

    if isinstance(obj, (PyArrowDataFrame, PyArrowSeries)):
        return "pyarrow"

    raise TypeError(f"Unsupported type: {type(obj).__module__}.{type(obj).__qualname__}")


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


def remove_dataframe_index(df: DataFrame) -> DataFrame:
    """Remove the index of a DataFrame and keep it as a regular column."""
    if isinstance(df, (PolarsDataFrame, PyArrowDataFrame)):
        return df

    if df.index.name is not None:
        df = df.reset_index(drop=False)

    return df


def re_set_dataframe_index(df: DataFrame, index_var: Optional[str] = None) -> DataFrame:
    """Remove existing dataframe index and set a new one if index_var is provided."""
    if isinstance(df, (PolarsDataFrame, PyArrowDataFrame)):
        return df

    if df.index.name is not None:
        df = df.reset_index(drop=False)

    if index_var is not None:
        # Cast column before setting it as index to prevent issue with PyArrow index in Dask dataframe
        if df[index_var].dtype.name == "timestamp[ns][pyarrow]":
            df[index_var] = df[index_var].astype("datetime64[ns]")

        df = df.set_index(index_var)

    return df


def re_set_dataframe_index_decorator(func: Callable) -> Callable:
    """Decorator to remove existing dataframe index and set a new one if index_var is provided."""

    def wrapper(*args, index_var: Optional[str] = None, **kwargs) -> DataFrame:
        df = func(*args, **kwargs)
        return re_set_dataframe_index(df, index_var=index_var)

    return wrapper


@re_set_dataframe_index_decorator
def _change_dataframe_backend_from_dask(
    df: DaskDataFrame,
    new_backend: Backend,
    **backend_kwargs,
) -> DataFrame:
    """Change the backend of a Dask dataframe."""
    if new_backend == "dask":
        return df

    if new_backend == "pandas":
        return df.compute(**backend_kwargs)

    if new_backend == "polars":
        backend_kwargs.setdefault("include_index", True)
        return pl.from_pandas(df.compute(), **backend_kwargs)

    if new_backend == "pyarrow":
        backend_kwargs.setdefault("preserve_index", True)
        return pa.Table.from_pandas(df.compute(), **backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


@re_set_dataframe_index_decorator
def _change_dataframe_backend_from_pandas(
    df: PandasDataFrame,
    new_backend: Backend,
    **backend_kwargs,
) -> DataFrame:
    """Change the backend of a Pandas dataframe."""
    if new_backend == "dask":
        return dd.from_pandas(df, **backend_kwargs)

    if new_backend == "pandas":
        return df

    if new_backend == "polars":
        backend_kwargs.setdefault("include_index", True)
        return pl.from_pandas(df, **backend_kwargs)

    if new_backend == "pyarrow":
        backend_kwargs.setdefault("preserve_index", True)
        return pa.Table.from_pandas(df, **backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


@re_set_dataframe_index_decorator
def _change_dataframe_backend_from_polars(
    df: PolarsDataFrame,
    new_backend: Backend,
    **backend_kwargs,
) -> DataFrame:
    """Change the backend of a Polars dataframe."""
    if new_backend == "dask":
        df = df.to_pandas(use_pyarrow_extension_array=True)
        return dd.from_pandas(df, **backend_kwargs)

    if new_backend == "pandas":
        backend_kwargs.setdefault("use_pyarrow_extension_array", True)
        df = df.to_pandas(**backend_kwargs)
        return df

    if new_backend == "polars":
        return df

    if new_backend == "pyarrow":
        return df.to_arrow(**backend_kwargs)

    raise ValueError(f"Unsupported backend: {new_backend}")


@re_set_dataframe_index_decorator
def _change_dataframe_backend_from_pyarrow(
    df: PyArrowDataFrame,
    new_backend: Backend,
    **backend_kwargs,
) -> DataFrame:
    """Change the backend of a PyArrow table."""
    if new_backend == "dask":
        df = df.to_pandas(types_mapper=pd.ArrowDtype)
        return dd.from_pandas(df, **backend_kwargs)

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
    index_var: Optional[str] = None,
    **backend_kwargs,
) -> Series:
    """Change the backend of a Dask series."""
    if index_var is not None:
        raise NotImplementedError("Setting an index is not supported for series.")

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
    index_var: Optional[str] = None,
    **backend_kwargs,
) -> Series:
    """Change the backend of a Pandas series."""
    if index_var is not None:
        raise NotImplementedError("Setting an index is not supported for series.")

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
    index_var: Optional[str] = None,
    **backend_kwargs,
) -> Series:
    """Change the backend of a Polars series."""
    if index_var is not None:
        raise NotImplementedError("Setting an index is not supported for series.")

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
    index_var: Optional[str] = None,
    **backend_kwargs,
) -> Series:
    """Change the backend of a Pyarrow series."""
    if index_var is not None:
        raise NotImplementedError("Setting an index is not supported for series.")

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


def cast_column_to_large_string(df: DataFrame, col: str) -> DataFrame:
    """Cast a column to a large string type."""
    if isinstance(df, (DaskDataFrame, PandasDataFrame)):
        df[col] = df[col].astype("large_string[pyarrow]")

    elif isinstance(df, PolarsDataFrame):
        df = df.cast({col: pl.String})

    elif isinstance(df, PyArrowDataFrame):
        schema = df.schema
        field_index = schema.get_field_index(col)
        schema = schema.remove(field_index)
        schema = schema.insert(field_index, pa.field(col, pa.large_string()))
        df = df.cast(target_schema=schema)

    return df
