#!/usr/bin/env python3
"""
Created on Mon Jun 12 17:57:40 2023.

@author: ghiggi
"""

import pandas as pd
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


def open_tslong(
    base_dir,
    ts_variables=None,
    start_time=None,
    end_time=None,
    tstore_ids=None,
    columns=None,
    filesystem=None,
    use_threads=True,
):
    """Open TStore into long-format into pandas.DataFrame.

    TSLONG.from_file(engine="pandas")
    """
    from tstore.tslong.pyarrow import open_tslong as pyarrow_reader

    # Read exploiting pyarrow
    # - We use pyarrow to avoid pandas copies at concatenation and join operations !
    tslong_pyarrow = pyarrow_reader(
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
    tslong_pandas = tslong_pyarrow.to_pandas(types_mapper=pd.ArrowDtype)
    return tslong_pandas


def to_tstore(
    df,
    # TSTORE options
    base_dir,
    # DFLONG attributes
    id_var,
    time_var,
    ts_variables,
    static_variables=None,
    # TSTORE options
    partitioning=None,
    tstore_structure="id-var",
    overwrite=True,
):
    """TSLONG.to_tstore()."""
    # If index time, remove
    if time_var not in df.columns:
        df = df.reset_index(names=time_var)

    # Set identifier as pyarrow large_string !
    # - Very important to join attrs at read time !
    df[id_var] = df[id_var].astype("large_string[pyarrow]")

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
    attr_cols = [id_var]
    if static_variables is not None:
        attr_cols += static_variables
    # - TODO: add flag to check if are actual duplicates !
    df_attrs = df[attr_cols]
    df_attrs = df_attrs.drop_duplicates(subset=id_var)
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
        id_var=id_var,
        time_var=time_var,
        ts_variables=list(ts_variables),
        tstore_structure=tstore_structure,
        partitioning=partitioning,
    )

    # Write to disk per identifier
    for tstore_id, df_group in df.groupby(id_var):
        for ts_variable, columns in ts_variables.items():
            # Retrieve columns of the TS object
            if columns is None:
                columns = [ts_variable]

            # Retrieve TS object
            df_ts = df_group[[*columns, time_var]].copy()
            df_ts = df_ts.reset_index(drop=True)

            # Check time is sorted ?
            # TODO

            # Add partitioning columns
            partitioning_str = partitioning[ts_variable]
            df_ts, partitions = add_partitioning_columns(
                df_ts,
                partitioning_str=partitioning_str,
                time_var=time_var,
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
