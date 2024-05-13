"""Module defining the TSDF abstract wrapper for a dataframe of TSArray objects."""

from tstore.backend import DataFrame, PandasDataFrame
from tstore.tswrapper.tswrapper import TSWrapper


class TSDF(TSWrapper):
    """Abstract wrapper for a dataframe of TSArray objects."""

    def __new__(cls, *args, **kwargs) -> "TSDF":
        """When calling TSDF() directly, return the appropriate subclass."""
        if cls is TSDF:
            df = kwargs.get("df", args[0])
            return TSDF.wrap(df)

        return super().__new__(cls)

    @staticmethod
    def wrap(df: DataFrame) -> "TSDF":
        """Wrap a DataFrame in the appropriate TSDF subclass."""
        # Lazy import to avoid circular imports
        from tstore.tsdf.pandas import TSDFPandas

        if isinstance(df, PandasDataFrame):
            return TSDFPandas(df)

        type_path = f"{type(df).__module__}.{type(df).__qualname__}"
        raise TypeError(f"Cannot wrap type {type_path} as a TSDF object.")
