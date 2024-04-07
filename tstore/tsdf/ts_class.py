#!/usr/bin/env python3
"""
Created on Mon Jun 12 22:17:59 2023.

@author: ghiggi
"""

import dask.dataframe as dd
import pandas as pd


def check_time_index(df):
    """Check pandas/dask index is datetime."""
    return pd.api.types.is_datetime64_any_dtype(df.index.dtype)


def add_partitioning_columns(df, partitioning_str):
    """Add partitioning columns to the dataframe."""
    # TODO: as function of TS_partitioning_string (YYYY/MM/DD) or (YY/DOY/HH)
    # dayofyear, dayofweek, hour,  minute, ...
    df["month"] = df.index.dt.month.values
    df["year"] = df.index.dt.year.values
    partition_on = ["year", "month"]
    return df, partition_on


def ensure_is_dask_dataframe(data):
    """Convert a table to a dask dataframe."""
    # Ensure object is a dask dataframe
    # - Dask Series does not have to_parquet method
    # TODO: generalize also for other object ... pandas, polars ...
    if isinstance(data, dd.Series):
        data = data.to_frame()
    return data


class TS:
    """TS object."""

    def __init__(self, data):
        """Initialize TS class."""
        # Set index as 'time'
        # --> TODO: this to adapt for polars, pyarrow ...
        data.index.name = "time"

        self.data = data

    def from_file(
        fpath,
        columns=None,
        start_time=None,
        end_time=None,
        split_row_groups=False,
        # TS class options (here dask.dataframe)
        calculate_divisions=True,
        ignore_metadata_file=False,
        **kwargs,
    ):
        """Read a time series from disk into a Dask.DataFrame."""
        # TODO: generalize to pyarrow, pandas, dask, polars
        from tstore.archive.ts.dask import open_ts

        df = open_ts(
            fpath,
            columns=columns,
            start_time=start_time,
            end_time=end_time,
            split_row_groups=split_row_groups,
            # Dask options
            calculate_divisions=calculate_divisions,
            ignore_metadata_file=ignore_metadata_file,
            **kwargs,
        )

        # Create the TS object
        return TS(df)

    def to_disk(self, fpath, partitioning_str=None):
        """Write TS object to disk."""
        # Ensure is a dask dataframe
        df = ensure_is_dask_dataframe(self.data)

        # Check the index is datetime
        check_time_index(df)

        # Check known divisions ?
        # - TODO: behaviour to determine
        if df.known_divisions:
            pass

        # Check time is sorted ?

        # Add partition columns
        df, partition_on = add_partitioning_columns(df, partitioning_str)

        # Write to Parquet
        df.to_parquet(
            fpath,
            engine="pyarrow",
            # Index option
            write_index=True,
            # Metadata
            custom_metadata=None,
            write_metadata_file=True,  # enable writing the _metadata file
            # File structure
            name_function=lambda i: f"part.{i}.parquet",  # default naming scheme
            partition_on=partition_on,
            # Encoding
            schema="infer",
            compression="snappy",
            # Writing options
            append=False,
            overwrite=False,
            ignore_divisions=False,
            compute=True,
        )
