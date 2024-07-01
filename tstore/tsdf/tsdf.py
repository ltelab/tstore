"""Module defining the TSDF abstract wrapper for a dataframe of TSArray objects."""

from abc import abstractmethod
from typing import TYPE_CHECKING

from tstore.backend import (
    Backend,
    DaskDataFrame,
    DataFrame,
    PandasDataFrame,
    PolarsDataFrame,
    PyArrowDataFrame,
    remove_dataframe_index,
)
from tstore.tsdf.ts_dtype import TSDtype
from tstore.tswrapper.tswrapper import TSWrapper

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.tslong import TSLong
    from tstore.tswide.tswide import TSWide


class TSDF(TSWrapper):
    """Abstract wrapper for a DataFrame of TSArray objects."""

    def __init__(
        self,
        df: DataFrame,
        id_var: str,
    ) -> None:
        """Wrap a DataFrame of TSArrays as a TSDF object.

        Args:
            df (DataFrame): DataFrame to wrap.
            id_var (str): Name of the column containing the identifier variable.
        """
        if not isinstance(df, PandasDataFrame):
            raise TypeError(
                "The input dataframe must be a Pandas DataFrame. Inner TS objects can contain dataframes of any type.",
            )

        df = remove_dataframe_index(df)
        super().__init__(df)
        # Set attributes using __dict__ to not trigger __setattr__
        self.__dict__.update(
            {
                "_tstore_id_var": id_var,
            },
        )

    def __new__(cls, *args, **kwargs) -> "TSDF":
        """When calling TSDF() directly, return the appropriate subclass."""
        if cls is TSDF:
            return TSDF.wrap(*args, **kwargs)

        return super().__new__(cls)

    def change_backend(self, new_backend: Backend) -> "TSDF":
        """Return a new TSDF object with dataframes wrapped in internal TS objects converted to a different backend."""
        df = self._obj.copy()
        ts_cols = _get_ts_columns(df)

        df[ts_cols] = df[ts_cols].applymap(lambda ts_obj: ts_obj.change_backend(new_backend))
        df[ts_cols] = df[ts_cols].astype(TSDtype)

        return self.wrap(df, self._tstore_id_var)

    @staticmethod
    @TSWrapper.copy_signature(__init__)
    def wrap(df: DataFrame, *args, **kwargs) -> "TSDF":
        """Wrap a DataFrame in the appropriate TSDF subclass."""
        # Lazy import to avoid circular imports
        from tstore.tsdf.dask import TSDFDask
        from tstore.tsdf.pandas import TSDFPandas
        from tstore.tsdf.polars import TSDFPolars
        from tstore.tsdf.pyarrow import TSDFPyArrow

        ts_cols = _get_ts_columns(df)
        ts_object = df[ts_cols[0]].iloc[0]
        inner_df = ts_object._obj

        if isinstance(inner_df, DaskDataFrame):
            return TSDFDask(df, *args, **kwargs)

        if isinstance(inner_df, PandasDataFrame):
            return TSDFPandas(df, *args, **kwargs)

        if isinstance(inner_df, PolarsDataFrame):
            return TSDFPolars(df, *args, **kwargs)

        if isinstance(inner_df, PyArrowDataFrame):
            return TSDFPyArrow(df, *args, **kwargs)

        type_path = f"{type(inner_df).__module__}.{type(inner_df).__qualname__}"
        raise TypeError(f"Cannot wrap type {type_path} as a TSDF object.")

    @abstractmethod
    def to_tslong(self) -> "TSLong":
        """Convert the wrapper into a TSLong object."""

    def to_tswide(self) -> "TSWide":
        """Convert the wrapper into a TSWide object."""
        return self.to_tslong().to_tswide()

    @property
    def _tstore_ts_vars(self) -> dict[str, list[str]]:
        """Return the dictionary of time-series column names."""
        df = self._obj
        ts_cols = _get_ts_columns(df)
        ts_objects = {col: df[col].iloc[0] for col in ts_cols}
        return {
            col: [var for var in ts_obj._obj.columns if var != ts_obj._tstore_time_var]
            for col, ts_obj in ts_objects.items()
        }

    @property
    def _tstore_static_vars(self) -> list[str]:
        """Return the list of static column names."""
        df = self._obj

        return [col for col in df.columns if col != self._tstore_id_var and not isinstance(df[col].dtype, TSDtype)]


def _get_ts_columns(df: PandasDataFrame) -> list[str]:
    """Return the list of columns containing TS objects."""
    return [col for col in df.columns if isinstance(df[col].dtype, TSDtype)]
