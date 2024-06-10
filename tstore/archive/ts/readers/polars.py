#!/usr/bin/env python3
"""
Created on Mon Jun 12 22:19:51 2023.

@author: ghiggi
"""

import polars as pl


def open_ts(
    fpath,
    partitions,
    start_time=None,  # noqa: ARG001
    end_time=None,  # noqa: ARG001
    # Options
    rechunk=True,
    use_statistics=True,
    hive_partitioning=True,
    storage_options=None,
    low_memory=False,
    # lazy option
    lazy=True,
    # in-memory only options
    columns=None,
    use_pyarrow=False,
    parallel=True,
):
    """Open a TS into a polars Frame."""
    # TODO: can we efficiently filter at parquet read time? see https://github.com/pola-rs/polars/issues/3964
    # https://docs.pola.rs/py-polars/html/reference/api/polars.read_parquet.html
    # https://docs.pola.rs/py-polars/html/reference/api/polars.scan_parquet.html
    if lazy:
        df_pl = pl.scan_parquet(
            fpath,
            rechunk=rechunk,
            storage_options=storage_options,
            hive_partitioning=hive_partitioning,
            use_statistics=use_statistics,
            low_memory=low_memory,
        )
        # Filter columns if not None
    else:
        df_pl = pl.read_parquet(
            fpath,
            rechunk=rechunk,
            storage_options=storage_options,
            hive_partitioning=hive_partitioning,
            use_statistics=use_statistics,
            low_memory=low_memory,
            # Others
            columns=columns,
            use_pyarrow=use_pyarrow,
            parallel=parallel,
        )

    # Filter by start_time and end_time
    # - TODO

    # Remove partitioning columns
    df_pl = df_pl.drop(partitions)

    return df_pl
