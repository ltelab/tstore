"""Module defining the TSLong abstract wrapper."""

from typing import TYPE_CHECKING, Optional, Union

from tstore.backend import (
    Backend,
    DaskDataFrame,
    DataFrame,
    PandasDataFrame,
    PolarsDataFrame,
    PyArrowDataFrame,
    cast_column_to_large_string,
    change_backend,
    get_column_names,
    get_dataframe_index,
    re_set_dataframe_index,
)
from tstore.tswrapper.tswrapper import TSWrapper

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tsdf.tsdf import TSDF
    from tstore.tswide.tswide import TSWide


class TSLong(TSWrapper):
    """Abstract wrapper for a long-form timeseries DataFrame."""

    def __init__(
        self,
        df: DataFrame,
        id_var: str,
        time_var: str = "time",
        ts_vars: Union[dict[str, list[str]], list[str], None] = None,
        static_vars: Optional[list[str]] = None,
        geometry_var: Optional[str] = None,
    ) -> None:
        """Wrap a long-form timeseries DataFrame as a TSLong object.

        Args:
            df (DataFrame): DataFrame to wrap.
            id_var (str): Name of the column containing the identifier variable.
            time_var (str): Name of the column containing the time variable. Defaults to "time".
            ts_vars (Union[dict[str, list[str]], list[str], None]): Dictionary of named groups of column names or list
                of column names (which will create one group per entry).
                Defaults to None, which will group all columns not in `static_vars` together under a group called
                "ts_variable".
            static_vars (Optional[list[str]]): List of column names that are static across time. Defaults to None.
            geometry_var (Optional[str]): Name of the column containing a geometry static over time. Defaults to None.
        """
        _check_id_var(id_var=id_var, df=df)
        _check_time_var(time_var=time_var, df=df, id_var=id_var)
        _check_geometry_var(geometry_var=geometry_var, df=df, id_var=id_var, time_var=time_var)

        if static_vars is None:
            static_vars = []
        else:
            _check_static_vars(
                static_vars=static_vars,
                df=df,
                id_var=id_var,
                time_var=time_var,
                geometry_var=geometry_var,
            )

        ts_vars = _ts_vars_as_checked_dict(
            ts_vars=ts_vars,
            df=df,
            id_var=id_var,
            time_var=time_var,
            static_vars=static_vars,
            geometry_var=geometry_var,
        )

        df = cast_column_to_large_string(df, id_var)

        # Ensure correct index column
        df = re_set_dataframe_index(df, index_var=time_var)

        super().__init__(df)

        # Set attributes using __dict__ to not trigger __setattr__
        self.__dict__.update(
            {
                "_tstore_id_var": id_var,
                "_tstore_time_var": time_var,
                "_tstore_ts_vars": ts_vars,
                "_tstore_static_vars": static_vars,
                "_tstore_geometry_var": geometry_var,
            },
        )

    def __new__(cls, *args, **kwargs) -> "TSLong":
        """When calling TSLong() directly, return the appropriate subclass."""
        if cls is TSLong:
            return TSLong.wrap(*args, **kwargs)

        return super().__new__(cls)

    def change_backend(self, new_backend: Backend) -> "TSLong":
        """Return a new wrapper with the dataframe converted to a different backend."""
        new_df = change_backend(self._obj, new_backend, index_var=self._tstore_time_var)
        return self._rewrap(new_df)

    @staticmethod
    @TSWrapper.copy_signature(__init__)
    def wrap(df: DataFrame, *args, **kwargs) -> "TSLong":
        """Wrap a DataFrame in the appropriate TSLong subclass.

        Takes the same arguments as the TSLong constructor.
        """
        # Lazy import to avoid circular imports
        from tstore.tslong.dask import TSLongDask
        from tstore.tslong.pandas import TSLongPandas
        from tstore.tslong.polars import TSLongPolars
        from tstore.tslong.pyarrow import TSLongPyArrow

        if isinstance(df, DaskDataFrame):
            return TSLongDask(df, *args, **kwargs)

        if isinstance(df, PandasDataFrame):
            return TSLongPandas(df, *args, **kwargs)

        if isinstance(df, PolarsDataFrame):
            return TSLongPolars(df, *args, **kwargs)

        if isinstance(df, PyArrowDataFrame):
            return TSLongPyArrow(df, *args, **kwargs)

        type_path = f"{type(df).__module__}.{type(df).__qualname__}"
        raise TypeError(f"Cannot wrap type {type_path} as a TSLong object.")

    def to_tsdf(self) -> "TSDF":
        """Convert the wrapper into a TSDF object."""
        dask_tslong = self.change_backend(new_backend="dask")
        dask_tsdf = dask_tslong.to_tsdf()
        tsdf = dask_tsdf.change_ts_backend(new_backend=self.current_backend)
        return tsdf

    def to_tswide(self) -> "TSWide":
        """Convert the wrapper into a TSWide object."""
        dask_tslong = self.change_backend(new_backend="dask")
        dask_tswide = dask_tslong.to_tswide()
        tswide = dask_tswide.change_backend(new_backend=self.current_backend)
        return tswide


def _check_id_var(id_var: str, df: DataFrame) -> None:
    """Check that the `id_var` argument is a column in the DataFrame.

    Raises
    ------
        ValueError: If the `id_var` argument is not a column in the DataFrame.
    """
    cols = get_column_names(df)

    if id_var not in cols:
        raise ValueError(f"Column name {id_var} is not available in the DataFrame.")


def _check_time_var(
    time_var: str,
    df: DataFrame,
    id_var: str,
) -> None:
    """Check that the `time_var` argument is a column in the DataFrame or the index.

    Raises
    ------
        ValueError: If the `time_var` argument is not an available column or the index in the DataFrame.
    """
    available_cols = set(get_column_names(df)) | {get_dataframe_index(df)} - {id_var}

    if time_var not in available_cols:
        raise ValueError(f"Column name {time_var} is not available in the DataFrame.")


def _check_geometry_var(
    geometry_var: Optional[str],
    df: DataFrame,
    id_var: str,
    time_var: str,
) -> None:
    """Check that the `geometry_var` argument is a column in the DataFrame, excluding `id_var` and `time_var`.

    Raises
    ------
        ValueError: If the `geometry_var` argument is not an available column in the DataFrame.
    """
    if geometry_var is None:
        return

    available_cols = set(get_column_names(df)) - {id_var, time_var}

    if geometry_var not in available_cols:
        raise ValueError(f"Column name {geometry_var} is not available in the DataFrame.")


def _check_static_vars(
    static_vars: list[str],
    df: DataFrame,
    id_var: str,
    time_var: str,
    geometry_var: Optional[str],
) -> None:
    """Check that the `static_vars` contains only columns available in the DataFrame, excluding `id_var` and `time_var`.

    Raises
    ------
        ValueError: If the `static_vars` argument contains column names not available in the DataFrame.
    """
    available_cols = set(get_column_names(df)) - {id_var, time_var}

    if geometry_var is not None:
        available_cols -= {geometry_var}

    if set(static_vars) - available_cols:
        raise ValueError(f"Column names {set(static_vars) - available_cols} are not available in the DataFrame.")


def _ts_vars_as_checked_dict(
    ts_vars: Union[dict[str, list[str]], list[str], None],
    df: DataFrame,
    id_var: str,
    time_var: str,
    static_vars: list[str],
    geometry_var: Optional[str],
) -> dict[str, list[str]]:
    """Convert the `ts_vars` argument to a dictionary if it is not already and check column names."""
    if ts_vars is None:
        return {
            "ts_variable": [
                col
                for col in get_column_names(df)
                if col != id_var and col != time_var and col not in static_vars and col != geometry_var
            ],
        }

    if isinstance(ts_vars, list):
        ts_vars = {col: [col] for col in ts_vars}

    _check_ts_vars(
        ts_vars=ts_vars,
        df=df,
        id_var=id_var,
        time_var=time_var,
        static_vars=static_vars,
        geometry_var=geometry_var,
    )

    return ts_vars


def _check_ts_vars(
    ts_vars: dict[str, list[str]],
    df: DataFrame,
    id_var: str,
    time_var: str,
    static_vars: list[str],
    geometry_var: Optional[str],
) -> None:
    """Check that the `ts_vars` argument does not contain repeated or unavailable column names.

    Raises
    ------
        ValueError: If the `ts_vars` argument contains repeated or unavailable column names.
    """
    available_cols = set(get_column_names(df)) - {id_var, time_var, geometry_var} - set(static_vars)

    requested_cols = set()
    for cols in ts_vars.values():
        new_cols = set(cols)
        if new_cols & requested_cols:
            raise ValueError(f"Column names {new_cols & available_cols} is duplicated in the `ts_vars` argument.")
        requested_cols.update(new_cols)

    if requested_cols - available_cols:
        raise ValueError(f"Column names {requested_cols - available_cols} are not available in the DataFrame.")

    if available_cols - requested_cols:
        raise ValueError(f"Column names {available_cols - requested_cols} are not specified in the arguments.")
