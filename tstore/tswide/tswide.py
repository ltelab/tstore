"""Module defining the TSWide abstract wrapper."""

from tstore.backend import DaskDataFrame, DataFrame, PandasDataFrame, PolarsDataFrame, PyArrowDataFrame
from tstore.tswrapper.tswrapper import TSWrapper


class TSWide(TSWrapper):
    """Abstract wrapper for a wide-form timeseries dataframe."""

    def __new__(cls, *args, **kwargs) -> "TSWide":
        """When calling TSWide() directly, return the appropriate subclass."""
        if cls is TSWide:
            df = kwargs.get("df", args[0])
            return TSWide.wrap(df)

        return super().__new__(cls)

    @staticmethod
    def wrap(df: DataFrame) -> "TSWide":
        """Wrap a DataFrame in the appropriate TSWide subclass."""
        # Lazy import to avoid circular imports
        from tstore.tswide.dask import TSWideDask
        from tstore.tswide.pandas import TSWidePandas
        from tstore.tswide.polars import TSWidePolars
        from tstore.tswide.pyarrow import TSWidePyArrow

        if isinstance(df, DaskDataFrame):
            return TSWideDask(df)

        if isinstance(df, PandasDataFrame):
            return TSWidePandas(df)

        if isinstance(df, PolarsDataFrame):
            return TSWidePolars(df)

        if isinstance(df, PyArrowDataFrame):
            return TSWidePyArrow(df)

        type_path = f"{type(df).__module__}.{type(df).__qualname__}"
        raise TypeError(f"Cannot wrap type {type_path} as a TSWide object.")
