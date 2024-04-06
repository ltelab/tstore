#!/usr/bin/env python3
"""
Created on Mon Jun 12 22:24:07 2023.

@author: ghiggi
"""
import dask.dataframe as dd
import pandas as pd

from tstore.archive.ts.filtering import get_time_filters
from tstore.archive.ts.partitioning import get_dataset_partitioning_columns


def open_ts(
    fpath,
    columns=None,
    start_time=None,
    end_time=None,
    split_row_groups=False,
    # Dask-specific
    calculate_divisions=True,
    ignore_metadata_file=False,
    **kwargs,
):
    """Open a time series into a Dask.DataFrame."""
    # Define filters argument
    filters = get_time_filters(start_time=start_time, end_time=end_time)

    # Define Apache Arrow settings
    arrow_to_pandas = {
        "zero_copy_only": False,  # Default is False. If True, raise error if doing copies
        "strings_to_categorical": False,
        "date_as_object": False,  # Default is True. If False convert to datetime64[ns]
        "timestamp_as_object": False,  # Default is True. If False convert to np.datetime64[ns]
        "use_threads": True,  #  parallelize the conversion using multiple threads.
        "safe": True,
        "split_blocks": False,
        "ignore_metadata": False,  # Default False. If False, use the 'pandas' metadata to get the Index
        "types_mapper": pd.ArrowDtype,  # Ensure pandas is created with Arrow dtype
    }

    # Read Apache Parquet
    df = dd.read_parquet(
        fpath,
        engine="pyarrow",
        dtype_backend="pyarrow",
        index=None,  # None --> Read back original time-index
        # Filtering
        columns=columns,  # Specify columns to load
        filters=filters,  # Row-filtering at read-time
        # Metadata options
        calculate_divisions=calculate_divisions,  # Calculate divisions from metadata
        ignore_metadata_file=ignore_metadata_file,  # True can slowdown a lot reading
        # Partitioning
        split_row_groups=split_row_groups,  # False --> Each file a partition
        # Arrow options
        arrow_to_pandas=arrow_to_pandas,
        # Other options,
        **kwargs,
    )

    # Drop partitioning columns
    partitioning_columns = get_dataset_partitioning_columns(fpath)
    df = df.drop(columns=partitioning_columns)

    return df
