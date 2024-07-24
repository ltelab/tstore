"""Module defining the TSDF abstract wrapper for a dataframe of TSArray objects."""

from typing import TYPE_CHECKING, Optional

import dask.dataframe as dd

from tstore.archive.metadata.readers import read_tstore_metadata
from tstore.backend import (
    Backend,
    DataFrame,
    PandasDataFrame,
    dataframe_types,
    remove_dataframe_index,
)
from tstore.tsdf.reader import _read_tsarrays
from tstore.tsdf.ts_dtype import TSDtype
from tstore.tsdf.writer import write_tstore
from tstore.tswrapper.tswrapper import TSWrapper

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.dask import TSLongDask
    from tstore.tslong.tslong import TSLong
    from tstore.tswide.tswide import TSWide


class TSDF(TSWrapper):
    """Wrapper for a DataFrame of TSArray objects."""

    def __init__(
        self,
        df: DataFrame,
        id_var: str,
    ) -> None:
        """Wrap a DataFrame of TSArrays as a TSDF object.

        Args:
            df (pd.DataFrame): DataFrame to wrap.
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

    def change_ts_backend(self, new_backend: Backend, ts_cols: Optional[list[str]] = None) -> "TSDF":
        """Return a new TSDF object with dataframes wrapped in internal TS objects converted to a different backend.

        Args:
            new_backend (Backend): New backend to use for the TS objects.
            ts_cols (Optional[list[str]]): List of columns to convert. If None, convert all TS columns.
        """
        df = self._obj.copy()
        all_ts_cols = get_ts_columns(df)

        if ts_cols is None:
            ts_cols = all_ts_cols

        inexistent_cols = set(ts_cols) - set(all_ts_cols)
        if inexistent_cols:
            raise ValueError(f"TS columns {inexistent_cols} do not exist in the TSDF object.")

        df[ts_cols] = df[ts_cols].map(lambda ts_obj: ts_obj.change_backend(new_backend))
        df[ts_cols] = df[ts_cols].astype(TSDtype(dataframe_types[new_backend]))

        return self.wrap(df, self._tstore_id_var)

    def get_ts_backend(self, ts_col: str) -> Backend:
        """Return the current backend of a wrapped dataframe."""
        df = self._obj.copy()
        ts_object = df[ts_col].iloc[0]
        return ts_object.current_backend

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

    def to_tstore(
        self,
        base_dir,
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,  # append functionality?
        # geometry
    ):
        """Write TStore from TSDF object."""
        tsdf = self.change_ts_backend(new_backend="dask")
        tsdf._to_tstore_dask(
            base_dir=base_dir,
            partitioning=partitioning,
            tstore_structure=tstore_structure,
            overwrite=overwrite,
        )

    def _to_tstore_dask(
        self,
        base_dir,
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,  # append functionality?
        # geometry
    ):
        """Write TStore from TSDF object with Dask backend for TS objects."""
        _ = write_tstore(
            self._obj,
            base_dir=base_dir,
            id_var=self._tstore_id_var,
            partitioning=partitioning,
            tstore_structure=tstore_structure,
            overwrite=overwrite,
        )

    @staticmethod
    def from_tstore(base_dir: str, backend: Backend = "dask") -> "TSDF":
        """Read TStore into TSDF object."""
        dask_tsdf = TSDF._from_tstore_dask(base_dir)

        if backend == "dask":
            return dask_tsdf

        return dask_tsdf.change_ts_backend(new_backend=backend)

    @staticmethod
    def _from_tstore_dask(base_dir: str) -> "TSDF":
        """Read TStore into TSDF object with Dask backend for TS objects."""
        # TODO: enable specify subset of TSArrays, attribute columns and rows to load
        # TODO: read_attributes using geopandas --> geoparquet
        # TODO: separate TSDF class if geoparquet (TSDF inherit from geopandas.GeoDataFrame ?)
        from tstore.archive.attributes.pandas import read_attributes

        # Read TStore metadata
        metadata = read_tstore_metadata(base_dir=base_dir)

        # Read TStore attributes
        df = read_attributes(base_dir).set_index(metadata["id_var"])

        # Get list of TSArrays
        list_ts_series = _read_tsarrays(base_dir, metadata)

        # Join TSArrays to dataframe
        for ts_series in list_ts_series:
            df = df.join(ts_series, how="left")
            #  pd.merge(df_attrs, df_series, left_index=True, right_index=True)

        # Return the TSDF
        return TSDF(
            df,
            id_var=metadata["id_var"],
        )

    # Method that return identifier column

    # Method that return the timeseries columns  (TSArrays)

    # Add compute method

    # Add wrappers to methods iloc, loc or join to return TSDF class

    # Remove methods that are not supported by TSArray
    # --> min, ...

    def to_tslong(self, backend: Backend = "dask") -> "TSLong":
        """Convert the wrapper into a TSLong object."""
        dask_tsdf = self.change_ts_backend(new_backend="dask")
        dask_tslong = dask_tsdf._to_tslong_dask()
        tslong = dask_tslong.change_backend(new_backend=backend)
        return tslong

    def _to_tslong_dask(self) -> "TSLongDask":
        """Convert the wrapper into a TSLong object."""
        from tstore.tslong.dask import TSLongDask

        df = None
        tstore_ids = self._obj[self._tstore_id_var].unique()

        long_rows = [self._get_long_rows(tstore_id) for tstore_id in tstore_ids]
        df = dd.concat(long_rows)
        time_var = df.index.name

        return TSLongDask(
            df,
            id_var=self._tstore_id_var,
            time_var=time_var,
            ts_vars=self._tstore_ts_vars,
            static_vars=self._tstore_static_vars,
        )

    def _get_long_rows(self, tstore_id: str) -> dd.DataFrame:
        """Return a long form DataFrame for a single tstore_id."""
        ts_df = self._obj
        ts_df = ts_df[ts_df[self._tstore_id_var] == tstore_id]
        ts_cols = get_ts_columns(ts_df)

        # Add time series
        df = None
        for ts_col in ts_cols:
            new_df = ts_df[ts_col].iloc[0]._obj
            df = new_df if df is None else df.join(new_df, how="outer")

        # Add static variables
        df = df.assign(**{self._tstore_id_var: tstore_id})

        for static_var in self._tstore_static_vars:
            static_value = ts_df[static_var].iloc[0]
            df = df.assign(**{static_var: static_value})

        return df

    def to_tswide(self, backend: Backend = "dask") -> "TSWide":
        """Convert the wrapper into a TSWide object."""
        return self.to_tslong(backend=backend).to_tswide()

    @property
    def _tstore_ts_vars(self) -> dict[str, list[str]]:
        """Return the dictionary of time-series column names."""
        df = self._obj
        ts_cols = get_ts_columns(df)
        ts_objects = {col: df[col].iloc[0] for col in ts_cols}
        return {
            col: [var for var in ts_obj._tstore_columns if var != ts_obj._tstore_time_var]
            for col, ts_obj in ts_objects.items()
        }

    @property
    def _tstore_static_vars(self) -> list[str]:
        """Return the list of static column names."""
        df = self._obj

        return [col for col in df.columns if col != self._tstore_id_var and not isinstance(df[col].dtype, TSDtype)]


def get_ts_columns(df: PandasDataFrame) -> list[str]:
    """Return the list of columns containing TS objects."""
    return [col for col in df.columns if isinstance(df[col].dtype, TSDtype)]
