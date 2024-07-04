"""Module defining the TSWide abstract wrapper."""

from abc import abstractmethod
from typing import TYPE_CHECKING, Optional

from tstore.backend import (
    DaskDataFrame,
    DataFrame,
    PandasDataFrame,
    PolarsDataFrame,
    PyArrowDataFrame,
)
from tstore.tswrapper.tswrapper import TSWrapper

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tsdf.tsdf import TSDF
    from tstore.tslong.tslong import TSLong


class TSWide(TSWrapper):
    """Abstract wrapper for a wide-form timeseries DataFrame."""

    def __init__(
        self,
        df: DataFrame,
        id_var: str,
        time_var: str = "time",
        ts_vars: Optional[dict[str, list[str]]] = None,
        static_vars: Optional[list[str]] = None,
    ) -> None:
        """Wrap a wide-form timeseries DataFrame as a TSWide object.

        Args:
            df (DataFrame): DataFrame to wrap.
            id_var (str): Name of the column containing the identifier variable.
            time_var (str): Name of the column containing the time variable. Defaults to "time".
            ts_vars (dict[str, list[str]]): Dictionary of named groups of column names.
                Defaults to None, which will group all columns not in `static_vars` together.
            static_vars (list[str]): List of column names that are static across time. Defaults to None.
        """
        # TODO: Cast id_var to large string
        # df = cast_column_to_large_string(df, id_var)

        # TODO: Ensure correct index column
        # df = re_set_dataframe_index(df, index_var=time_var)

        super().__init__(df)

        if static_vars is None:
            static_vars = []

        if ts_vars is None:
            ts_vars = {
                "ts_variable": [
                    col for col in df.columns if col != id_var and col != time_var and col not in static_vars
                ],
            }

        # Set attributes using __dict__ to not trigger __setattr__
        self.__dict__.update(
            {
                "_tstore_id_var": id_var,
                "_tstore_time_var": time_var,
                "_tstore_ts_vars": ts_vars,
                "_tstore_static_vars": static_vars,
            },
        )

    def __new__(cls, *args, **kwargs) -> "TSWide":
        """When calling TSWide() directly, return the appropriate subclass."""
        if cls is TSWide:
            df = kwargs.get("df", args[0])
            return TSWide.wrap(df)

        return super().__new__(cls)

    @staticmethod
    def wrap(df: DataFrame, *args, **kwargs) -> "TSWide":
        """Wrap a DataFrame in the appropriate TSWide subclass."""
        # Lazy import to avoid circular imports
        from tstore.tswide.dask import TSWideDask
        from tstore.tswide.pandas import TSWidePandas
        from tstore.tswide.polars import TSWidePolars
        from tstore.tswide.pyarrow import TSWidePyArrow

        if isinstance(df, DaskDataFrame):
            return TSWideDask(df, *args, **kwargs)

        if isinstance(df, PandasDataFrame):
            return TSWidePandas(df, *args, **kwargs)

        if isinstance(df, PolarsDataFrame):
            return TSWidePolars(df, *args, **kwargs)

        if isinstance(df, PyArrowDataFrame):
            return TSWidePyArrow(df, *args, **kwargs)

        type_path = f"{type(df).__module__}.{type(df).__qualname__}"
        raise TypeError(f"Cannot wrap type {type_path} as a TSWide object.")

    def to_tsdf(self) -> "TSDF":
        """Convert the wrapper into a TSDF object."""
        return self.to_tslong().to_tsdf()

    @abstractmethod
    def to_tslong(self) -> "TSLong":
        """Convert the wrapper into a TSLong object."""
