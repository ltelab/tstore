"""Module defining the TSDF abstract wrapper for a dataframe of TSArray objects."""

from abc import abstractmethod
from typing import TYPE_CHECKING

from tstore.backend import DataFrame, PandasDataFrame
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
        time_var: str = "time",
    ) -> None:
        """Wrap a DataFrame of TSArrays as a TSDF object.

        Args:
            df (DataFrame): DataFrame to wrap.
            id_var (str): Name of the column containing the identifier variable.
            time_var (str): Name of the column containing the time variable. Defaults to "time".
        """
        super().__init__(df)
        # Set attributes using __dict__ to not trigger __setattr__
        self.__dict__.update(
            {
                "_tstore_id_var": id_var,
                "_tstore_time_var": time_var,
            },
        )

    def __new__(cls, *args, **kwargs) -> "TSDF":
        """When calling TSDF() directly, return the appropriate subclass."""
        if cls is TSDF:
            return TSDF.wrap(*args, **kwargs)

        return super().__new__(cls)

    @staticmethod
    @TSWrapper.copy_signature(__init__)
    def wrap(df: DataFrame, *args, **kwargs) -> "TSDF":
        """Wrap a DataFrame in the appropriate TSDF subclass."""
        # Lazy import to avoid circular imports
        from tstore.tsdf.pandas import TSDFPandas

        if isinstance(df, PandasDataFrame):
            return TSDFPandas(df, *args, **kwargs)

        type_path = f"{type(df).__module__}.{type(df).__qualname__}"
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
        ts_cols = [col for col in df.columns if isinstance(df[col].dtype, TSDtype)]
        ts_objects = {col: df[col].iloc[0] for col in ts_cols}
        return {col: list(ts_obj.data.columns) for col, ts_obj in ts_objects.items()}

    @property
    def _tstore_static_vars(self) -> list[str]:
        """Return the list of static column names."""
        df = self._obj

        return [
            col
            for col in df.columns
            if col != self._tstore_id_var and col != self._tstore_time_var and not isinstance(df[col].dtype, TSDtype)
        ]
