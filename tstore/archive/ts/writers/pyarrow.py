#!/usr/bin/env python3
"""
Created on Mon Apr  8 17:26:02 2024.

@author: ghiggi
"""

import math
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_partitioned_dataset(
    base_dir,
    table,
    partitioning=None,
    row_group_size="400MB",
    max_file_size="2GB",
    compression="snappy",
    compression_level=None,
    # Computing options
    max_open_files=0,
    use_threads=True,
):
    """Write partitioned Parquet Dataset.

    https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html
    """
    # row_group_size="400MB"
    # max_file_size="2GB"
    # compression="snappy"
    # compression_level=None
    # # Computing options
    # max_open_files=0
    # use_threads=True

    if partitioning is None:
        partitioning = []

    # Estimate row_group_size (in number of rows)
    if isinstance(row_group_size, str):  # "200 MB"
        row_group_size = estimate_row_group_size(table, size=row_group_size)
        max_rows_per_group = row_group_size
        min_rows_per_group = row_group_size

    # Estimate maximum number of file row (in number of rows)
    max_rows_per_file = estimate_row_group_size(table, size=max_file_size)

    # Define table schema (without partitioning columns)
    table_schema = table.drop_columns(partitioning).schema

    # Define file visitor for metadata collection
    metadata_collector = []

    def file_visitor(written_file):
        metadata_collector.append(written_file.metadata)

    # Define file options
    file_options = {}
    file_options["compression"] = compression
    file_options["compression_level"] = compression_level
    file_options["write_statistics"] = True

    parquet_format = pa.dataset.ParquetFileFormat()
    file_options = parquet_format.make_write_options(**file_options)

    # Rewrite dataset
    pa.dataset.write_dataset(
        table,
        base_dir=base_dir,
        format="parquet",
        partitioning=partitioning,
        partitioning_flavor="hive",  # TODO: maybe enable DirectoryPartitioning and store info on metadata.yaml file
        # Directory options
        create_dir=True,
        existing_data_behavior="overwrite_or_ignore",
        # Options
        use_threads=use_threads,
        file_options=file_options,
        file_visitor=file_visitor,
        # Options for files size/rows
        max_rows_per_file=max_rows_per_file,
        min_rows_per_group=min_rows_per_group,
        max_rows_per_group=max_rows_per_group,
        # Options to control open connections
        max_open_files=max_open_files,
    )

    # Maybe only to write if partitioning is not None?

    # Write the ``_common_metadata`` parquet file without row groups statistics
    pq.write_metadata(table_schema, os.path.join(base_dir, "_common_metadata"))

    # Write the ``_metadata`` parquet file with row groups statistics of all files
    pq.write_metadata(
        table_schema,
        os.path.join(base_dir, "_metadata"),
        metadata_collector=metadata_collector,
    )


def convert_size_to_bytes(size):
    """Convert size to bytes."""
    if not isinstance(size, (str, int)):
        raise TypeError("Expecting a string (i.e. 200MB) or the integer number of bytes.")
    if isinstance(size, int):
        return size
    try:
        size = _convert_size_to_bytes(size)
    except Exception:
        raise ValueError("Impossible to parse {size_str} to the number of bytes.")
    return size


def estimate_row_group_size(df, size="200MB"):
    """Estimate row_group_size parameter based on the desired row group memory size.

    row_group_size is a Parquet argument controlling the number of rows
    in each Apache Parquet File Row Group.
    """
    if isinstance(df, pa.Table):
        memory_used = df.nbytes
    elif isinstance(df, pd.DataFrame):
        memory_used = df.memory_usage().sum()
    else:
        raise NotImplementedError("Unrecognized dataframe type")
    size_bytes = convert_size_to_bytes(size)
    n_rows = len(df)
    memory_per_row = memory_used / n_rows
    return math.floor(size_bytes / memory_per_row)


def _convert_size_to_bytes(size_str):
    """Convert human filesizes to bytes.

    Special cases:
     - singular units, e.g., "1 byte"
     - byte vs b
     - yottabytes, zetabytes, etc.
     - with & without spaces between & around units.
     - floats ("5.2 mb")

    To reverse this, see hurry.filesize or the Django filesizeformat template
    filter.

    :param size_str: A human-readable string representing a file size, e.g.,
    "22 megabytes".
    :return: The number of bytes represented by the string.
    """
    multipliers = {
        "kilobyte": 1024,
        "megabyte": 1024**2,
        "gigabyte": 1024**3,
        "terabyte": 1024**4,
        "petabyte": 1024**5,
        "exabyte": 1024**6,
        "zetabyte": 1024**7,
        "yottabyte": 1024**8,
        "kb": 1024,
        "mb": 1024**2,
        "gb": 1024**3,
        "tb": 1024**4,
        "pb": 1024**5,
        "eb": 1024**6,
        "zb": 1024**7,
        "yb": 1024**8,
    }

    for suffix in multipliers:
        size_str = size_str.lower().strip().strip("s")
        if size_str.lower().endswith(suffix):
            return int(float(size_str[0 : -len(suffix)]) * multipliers[suffix])
    if size_str.endswith("b"):
        size_str = size_str[0:-1]
    elif size_str.endswith("byte"):
        size_str = size_str[0:-4]
    return int(size_str)


# def test_filesize_conversions(self):
#         """Can we convert human filesizes to bytes?"""
#         qa_pairs = [
#             ('58 kb', 59392),
#             ('117 kb', 119808),
#             ('117kb', 119808),
#             ('1 byte', 1),
#             ('1 b', 1),
#             ('117 bytes', 117),
#             ('117  bytes', 117),
#             ('  117 bytes  ', 117),
#             ('117b', 117),
#             ('117bytes', 117),
#             ('1 kilobyte', 1024),
#             ('117 kilobytes', 119808),
#             ('0.7 mb', 734003),
#             ('1mb', 1048576),
#             ('5.2 mb', 5452595),
#         ]
#         for qa in qa_pairs:
#             print("Converting '%s' to bytes..." % qa[0], end='')
#             self.assertEqual(convert_size_to_bytes(qa[0]), qa[1])
#             print('✓')
