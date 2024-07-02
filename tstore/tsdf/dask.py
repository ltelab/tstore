"""TSDF class wrapping a Dask dataframe of TSArray objects."""

from typing import TYPE_CHECKING

import dask.dataframe as dd

from tstore.archive.metadata.readers import read_tstore_metadata
from tstore.tsdf.reader import _read_tsarrays
from tstore.tsdf.tsdf import TSDF, get_ts_columns
from tstore.tsdf.writer import write_tstore

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.dask import TSLongDask


class TSDFDask(TSDF):
    """A dataframe class with additional functionality for TSArray data."""

    def to_tstore(
        self,
        base_dir,
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,  # append functionality?
        # geometry
    ):
        """Write TStore from TSDF object."""
        _ = write_tstore(
            self._obj,
            base_dir=base_dir,
            id_var=self._tstore_id_var,
            partitioning=partitioning,
            tstore_structure=tstore_structure,
            overwrite=overwrite,
        )

    @staticmethod
    def from_tstore(base_dir: str) -> "TSDFDask":
        """Read TStore into TSDF object."""
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
        return TSDFDask(
            df,
            id_var=metadata["id_var"],
        )

    # Method that return identifier column

    # Method that return the timeseries columns  (TSArrays)

    # Add compute method

    # Add wrappers to methods iloc, loc or join to return TSDF class

    # Remove methods that are not supported by TSArray
    # --> min, ...

    def to_tslong(self) -> "TSLongDask":
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
