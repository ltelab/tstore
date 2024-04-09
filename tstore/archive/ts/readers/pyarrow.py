#!/usr/bin/env python3
"""
Created on Mon Jun 12 22:19:51 2023.

@author: ghiggi
"""
import pyarrow.parquet as pq
from tstore.archive.ts.utility import get_time_filters
 

def open_ts(
    fpath,
    partitions,
    start_time=None,
    end_time=None,
    columns=None,
    split_row_groups=False,
    # pyarrow-specific
    filesystem=None,
    use_threads=True,
):
    """Open a TS into a pyarrow.Table."""
    # Define filters argument
    filters = get_time_filters(start_time=start_time, end_time=end_time)

    # Read Option 1
    # - https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html#pyarrow-parquet-read-table
    table = pq.read_table(
        fpath,
        use_pandas_metadata=True,
        columns=columns,
        filters=filters,
        filesystem=filesystem,
        use_threads=use_threads,
    )
    # Read Option 2
    # - https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
    # Create a ParquetDataset object
    # dataset = pq.ParquetDataset(fpath,
    #                             filters=filters,
    #                             filesystem=filesystem,
    #                             # Specific to ParquetDataset
    #                             # metadata_nthreads=1,
    #                             split_row_groups=split_row_groups,

    # )
    # table = dataset.read(columns=columns,
    #                       use_pandas_metadata=True,
    #                       use_threads=use_threads,
    #
    # )

    # Remove partitioning columns
    table = table.drop(partitions)
    return table
