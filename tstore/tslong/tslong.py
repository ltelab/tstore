"""Module defining the TSLong abstract wrapper."""

from typing import Optional

from tstore.backend import DaskDataFrame, DataFrame, PandasDataFrame, PolarsDataFrame, PyArrowDataFrame
from tstore.tswrapper.tswrapper import TSWrapper


class TSLong(TSWrapper):
    """Abstract wrapper for a long-form timeseries dataframe."""

    def __init__(
        self,
        df: DataFrame,
        id_var: str,
        time_var: str = "time",
        ts_vars: Optional[dict[str, list[str]]] = None,
        static_vars: Optional[list[str]] = None,
    ) -> None:
        """Wrap a long-form timeseries DataFrame as a TSLong object.

        Args:
            df (DataFrame): DataFrame to wrap.
            id_var (str): Name of the column containing the identifier variable.
            time_var (str): Name of the column containing the time variable. Defaults to "time".
            ts_vars (dict[str, list[str]]): Dictionary of named groups of column names.
                Defaults to None, which will group all columns not in `static_vars` together.
            static_vars (list[str]): List of column names that are static across time. Defaults to None.
        """
        super().__init__(df)
        # Set attributes using __dict__ to not trigger __setattr__
        self.__dict__.update(
            {
                "_tstore_id_var": id_var,
                "_tstore_time_var": time_var,
                "_tstore_ts_vars": ts_vars,
                "_tstore_static_vars": static_vars,
            },
        )

    def __new__(cls, *args, **kwargs) -> "TSLong":
        """When calling TSLong() directly, return the appropriate subclass."""
        if cls is TSLong:
            return TSLong.wrap(*args, **kwargs)

        return super().__new__(cls)

    @staticmethod
    def wrap(df: DataFrame, *args, **kwargs) -> "TSLong":
        """Wrap a DataFrame in the appropriate TSLong subclass."""
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
