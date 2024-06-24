"""TSDF class wrapping a Pandas dataframe of TSArray objects."""

from pathlib import Path

from tstore.archive.metadata.readers import read_tstore_metadata
from tstore.tsdf.reader import _read_tsarrays
from tstore.tsdf.tsdf import TSDF
from tstore.tsdf.writer import write_tstore


class TSDFPandas(TSDF):
    """A dataframe class with additional functionality for TSArray data."""

    def to_tstore(
        self,
        base_dir: Path | str,
        id_var: str | None = None,
        time_var: str | None = None,  # TODO: likely not needed !
        partitioning: str | None = None,
        tstore_structure: str = "id-var",
        var_prefix: str = "variable",
        overwrite: bool = True,  # append functionality?
        # geometry
    ) -> None:
        """
        Write TSDF into a TStore.

        Parameters
        ----------
        base_dir : path-like
            Base directory of the TStore.
        id_var : str, optional
            Name of the id variable.
        time_var : str, optional
            Name of the time variable.
        ts_variables : list-like of str
            List of time series variables to write.
        static_variables : list-like of str, optional
            List of static variables to write.
        partitioning : str, optional
            Time partitioning string.
        tstore_structure : ["id-var", "var-id"], default "id-var"
            TStore structure, either "id-var" or "var-id".
        var_prefix : str, default "variable"
            Prefix for the variable directory in the TStore.
        overwrite : bool, default True
            Overwrite existing TStore.
        """
        write_tstore(
            self._df,
            base_dir=base_dir,
            id_var=id_var,
            time_var=time_var,
            partitioning=partitioning,
            tstore_structure=tstore_structure,
            var_prefix=var_prefix,
            overwrite=overwrite,
        )

    @staticmethod
    def from_tstore(base_dir: Path | str, var_prefix: str = "variable") -> "TSDFPandas":
        """
        Read TStore into TSDF object.

        Parameters
        ----------
        base_dir : path-like
            Base directory of the TStore.
        var_prefix : str, default "variable"
            Prefix for the variable directory in the TStore.

        Returns
        -------
        TSDFPandas
            TSDF object with pandas backend.
        """
        # TODO: enable specify subset of TSArrays, attribute columns and rows to load
        # TODO: read_attributes using geopandas --> geoparquet
        # TODO: separate TSDF class if geoparquet (TSDF inherit from geopandas.GeoDataFrame ?)
        from tstore.archive.attributes.pandas import read_attributes

        # Read TStore metadata
        metadata = read_tstore_metadata(base_dir=base_dir)

        # Read TStore attributes
        df = read_attributes(base_dir).set_index(metadata["id_var"])

        # Get list of TSArrays
        list_ts_series = _read_tsarrays(base_dir, metadata, var_prefix)

        # Join TSArrays to dataframe
        for ts_series in list_ts_series:
            df = df.join(ts_series, how="left")
            #  pd.merge(df_attrs, df_series, left_index=True, right_index=True)

        # Return the TSDF
        return TSDFPandas(df)

    # Method that return identifier column

    # Method that return the timeseries columns  (TSArrays)

    # Add compute method

    # Add wrappers to methods iloc, loc or join to return TSDF class

    # Remove methods that are not supported by TSArray
    # --> min, ...
