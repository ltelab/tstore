"""Module defining the TSLong abstract wrapper."""

from abc import abstractmethod
from typing import TYPE_CHECKING, Optional

from tstore.backend import (
    Backend,
    DaskDataFrame,
    DataFrame,
    PandasDataFrame,
    PolarsDataFrame,
    PyArrowDataFrame,
    change_backend,
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

        if static_vars is None:
            static_vars = []

        if ts_vars is None:
            ts_vars = {
                "ts_variable": [
                    col for col in df.columns if col != id_var and col != time_var and col not in static_vars
                ],
            }

        # TODO: Adapt to Polar and PyArrow
        if isinstance(df, (DaskDataFrame, PandasDataFrame)):
            df[id_var] = df[id_var].astype("large_string[pyarrow]")

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

    @abstractmethod
    def to_tsdf(self) -> "TSDF":
        """Convert the wrapper into a TSDF object."""

    @abstractmethod
    def to_tswide(self) -> "TSWide":
        """Convert the wrapper into a TSWide object."""
