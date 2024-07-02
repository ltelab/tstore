"""Module defining the TSLongPandas wrapper."""

from typing import TYPE_CHECKING

import pyarrow as pa

from tstore.archive.io import (
    check_tstore_directory,
    check_tstore_structure,
    define_attributes_filepath,
    define_tsarray_filepath,
)
from tstore.archive.metadata.writers import write_tstore_metadata
from tstore.archive.partitions import add_partitioning_columns, check_partitioning
from tstore.archive.ts.writers.pyarrow import write_partitioned_dataset
from tstore.tslong.pyarrow import TSLongPyArrow
from tstore.tslong.tslong import TSLong

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tsdf.pandas import TSDFPandas
    from tstore.tswide.pandas import TSWidePandas


class TSLongPandas(TSLong):
    """Wrapper for a long-form Pandas timeseries dataframe."""

    def to_tstore(
        self,
        base_dir,
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,
    ):
        """Write the wrapped dataframe as a TStore structure."""
        # If index time, remove
        df = self._obj
        if self._tstore_time_var not in df.columns:
            df = df.reset_index(names=self._tstore_time_var)

        # Check inputs
        tstore_structure = check_tstore_structure(tstore_structure)
        base_dir = check_tstore_directory(base_dir, overwrite=overwrite)

        # Check ts_variables
        ts_variables = self._tstore_ts_vars
        if isinstance(ts_variables, list):
            ts_variables = {column: None for column in ts_variables}

        # Identify all ts columns
        # ts_columns = set(df.columns) - set([self._tstore_time_var]) - set(static_variables)

        # Check which columns remains (not specified at class init)
        # TODO

        # Check partitioning
        partitioning = check_partitioning(partitioning, ts_variables=list(ts_variables))

        # Identify static dataframe (attributes)
        attr_cols = [self._tstore_id_var]
        if self._tstore_static_vars is not None:
            attr_cols += self._tstore_static_vars
        # - TODO: add flag to check if are actual duplicates !
        df_attrs = df[attr_cols]
        df_attrs = df_attrs.drop_duplicates(subset=self._tstore_id_var)
        df_attrs = df_attrs.reset_index(drop=True)

        # Write static attributes
        # --> Need to test that save id_var as large_string !
        attrs_fpath = define_attributes_filepath(base_dir)
        df_attrs.to_parquet(
            attrs_fpath,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        # Write tstore metadata
        write_tstore_metadata(
            base_dir=base_dir,
            id_var=self._tstore_id_var,
            ts_variables=list(ts_variables),
            tstore_structure=tstore_structure,
            partitioning=partitioning,
        )

        # Write to disk per identifier
        for tstore_id, df_group in df.groupby(self._tstore_id_var):
            for ts_variable, columns in ts_variables.items():
                # Retrieve columns of the TS object
                if columns is None:
                    columns = [ts_variable]

                # Retrieve TS object
                df_ts = df_group[[*columns, self._tstore_time_var]].copy()
                df_ts = df_ts.reset_index(drop=True)

                # Check time is sorted ?
                # TODO

                # Add partitioning columns
                partitioning_str = partitioning[ts_variable]
                df_ts, partitions = add_partitioning_columns(
                    df_ts,
                    partitioning_str=partitioning_str,
                    time_var=self._tstore_time_var,
                    backend="pandas",
                )

                # Define filepath of partitioned TS
                ts_fpath = define_tsarray_filepath(
                    base_dir=base_dir,
                    tstore_id=tstore_id,
                    ts_variable=ts_variable,
                    tstore_structure=tstore_structure,
                )

                # -----------------------------------------------
                # Maybe wrap df_ts into TS and then use write_partitioned_dataset within TS.to_parquet

                # Retrieve pyarrow Table
                table = pa.Table.from_pandas(df_ts)
                write_partitioned_dataset(
                    base_dir=ts_fpath,
                    table=table,
                    partitioning=partitions,
                    # row_group_size="400MB",
                    # max_file_size="2GB",
                    # compression="snappy",
                    # compression_level=None,
                    # # Computing options
                    # max_open_files=0,
                    # use_threads=True,
                )

    @staticmethod
    def from_tstore(
        base_dir,
        ts_variables=None,
        start_time=None,
        end_time=None,
        tstore_ids=None,
        columns=None,
        filesystem=None,
        use_threads=True,
    ) -> "TSLongPandas":
        """Open a TStore file structure as a TSLongPandas wrapper around a Pandas long dataframe."""
        # Read exploiting pyarrow
        # - We use pyarrow to avoid pandas copies at concatenation and join operations !
        tslong_pyarrow = TSLongPyArrow.from_tstore(
            base_dir,
            ts_variables=ts_variables,
            start_time=start_time,
            end_time=end_time,
            tstore_ids=tstore_ids,
            columns=columns,
            filesystem=filesystem,
            use_threads=use_threads,
        )

        # Conversion to pandas
        return tslong_pyarrow.change_backend(new_backend="pandas")

    def to_tsdf(self) -> "TSDFPandas":
        """Convert the wrapper into a TSDF object."""
        dask_tslong = self.change_backend(new_backend="dask")
        dask_tsdf = dask_tslong.to_tsdf()
        tsdf = dask_tsdf.change_backend(new_backend="pandas")
        return tsdf

    def to_tswide(self) -> "TSWidePandas":
        """Convert the wrapper into a TSWide object."""
        raise NotImplementedError
