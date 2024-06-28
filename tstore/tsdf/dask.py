"""TSDF class wrapping a Dask dataframe of TSArray objects."""

from typing import TYPE_CHECKING

from tstore.archive.metadata.readers import read_tstore_metadata
from tstore.tsdf.reader import _read_tsarrays
from tstore.tsdf.tsdf import TSDF
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
            time_var=self._tstore_time_var,
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
            time_var=metadata["time_var"],
        )

    # Method that return identifier column

    # Method that return the timeseries columns  (TSArrays)

    # Add compute method

    # Add wrappers to methods iloc, loc or join to return TSDF class

    # Remove methods that are not supported by TSArray
    # --> min, ...

    def to_tslong(self) -> "TSLongDask":
        """Convert the wrapper into a TSLong object."""
        raise NotImplementedError
