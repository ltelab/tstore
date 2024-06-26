"""Module defining the TSLong abstract wrapper."""

from tstore.backend import DaskDataFrame, DataFrame, PandasDataFrame, PolarsDataFrame, PyArrowDataFrame
from tstore.tswrapper.tswrapper import TSWrapper


class TSLong(TSWrapper):
    """Abstract wrapper for a long-form timeseries dataframe."""

    def __new__(cls, *args, **kwargs) -> "TSLong":
        """When calling TSLong() directly, return the appropriate subclass."""
        if cls is TSLong:
            df = kwargs.get("df", args[0])
            return TSLong.wrap(df)

        return super().__new__(cls)

    @staticmethod
    def wrap(df: DataFrame) -> "TSLong":
        """Wrap a DataFrame in the appropriate TSLong subclass."""
        # Lazy import to avoid circular imports
        from tstore.tslong.dask import TSLongDask
        from tstore.tslong.pandas import TSLongPandas
        from tstore.tslong.polars import TSLongPolars
        from tstore.tslong.pyarrow import TSLongPyArrow

        if isinstance(df, DaskDataFrame):
            return TSLongDask(df)

        if isinstance(df, PandasDataFrame):
            return TSLongPandas(df)

        if isinstance(df, PolarsDataFrame):
            return TSLongPolars(df)

        if isinstance(df, PyArrowDataFrame):
            return TSLongPyArrow(df)

        type_path = f"{type(df).__module__}.{type(df).__qualname__}"
        raise TypeError(f"Cannot wrap type {type_path} as a TSLong object.")
