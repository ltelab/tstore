"""Module defining the TSLongPolars wrapper."""

import pandas as pd
import polars as pl

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


class TSLongPolars(TSLong):
    """Wrapper for a long-form Polars timeseries dataframe."""

    def to_tstore(
        self,
        # TSTORE options
        base_dir,
        # DFLONG attributes
        id_var,
        time_var,
        ts_variables,
        static_variables,
        # TSTORE options
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,
    ) -> None:
        """Write the wrapped dataframe as a TStore structure."""
        # Check inputs
        tstore_structure = check_tstore_structure(tstore_structure)
        base_dir = check_tstore_directory(base_dir, overwrite=overwrite)

        # Check ts_variables
        if isinstance(ts_variables, list):
            ts_variables = {column: None for column in ts_variables}

        # Identify all ts columns
        # ts_columns = set(df.columns) - set([time_var]) - set(static_variables)

        # Check which columns remains (not specified at class init)
        # TODO

        # Check partitioning
        partitioning = check_partitioning(partitioning, ts_variables=list(ts_variables))

        # Identify static dataframe (attributes)
        # - TODO: add flag to check if are actual duplicates !
        df_attrs = self._df[[id_var, *static_variables]]
        df_attrs = df_attrs.unique()

        # Write static attributes
        # --> Need to test that save id_var as large_string !
        attrs_fpath = define_attributes_filepath(base_dir)

        # Conversion to pyarrow table
        # TODO: use https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html

        # Conversion to pandas
        df_pd = df_attrs.to_arrow().to_pandas(types_mapper=pd.ArrowDtype)
        df_pd.to_parquet(
            attrs_fpath,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        # Write tstore metadata
        write_tstore_metadata(
            base_dir=base_dir,
            id_var=id_var,
            time_var=time_var,
            ts_variables=list(ts_variables),
            tstore_structure=tstore_structure,
            partitioning=partitioning,
        )

        # Write to disk per identifier
        for tstore_id, df_group in self._df.groupby(id_var):
            for ts_variable, columns in ts_variables.items():
                # Retrieve columns of the TS object
                if columns is None:
                    columns = [ts_variable]

                # Retrieve TS object
                df_ts = df_group[[*columns, time_var]]

                # Check time is sorted ?
                # TODO

                # Add partitioning columns
                partitioning_str = partitioning[ts_variable]
                df_ts, partitions = add_partitioning_columns(
                    df_ts,
                    partitioning_str=partitioning_str,
                    time_var=time_var,
                    backend="polars",
                )

                # Define filepath of partitioned TS
                ts_fpath = define_tsarray_filepath(
                    base_dir=base_dir,
                    tstore_id=tstore_id,
                    ts_variable=ts_variable,
                    tstore_structure=tstore_structure,
                )

                # TODO; Maybe create TS object and use TS.to_parquet() once implemented
                # Retrieve pyarrow Table
                table = df_ts.to_arrow()

                # Write partitioned dataset
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
    ) -> "TSLongPolars":
        """Open a TStore file structure as a TSLongPolar wrapper around a Polar long dataframe."""
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
        )._df

        # Conversion to polars
        tslong_pl = pl.from_arrow(tslong_pyarrow)
        return TSLongPolars(tslong_pl)
