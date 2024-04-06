#!/usr/bin/env python3
"""
Created on Mon Jun 12 23:22:16 2023.

@author: ghiggi
"""
import pyarrow.parquet as pq

from tstore.archive.checks import check_is_tstore, check_tstore_ids
from tstore.archive.io import define_attributes_filepath


def get_tstore_ids_filters(tstore_ids=None):
    """Define filters for Parquet Dataset subsetting at read-time."""
    # TODO implement logic
    filters = None
    return filters


def read_attributes(base_dir, tstore_ids=None, filesystem=None, use_threads=True):
    """Read TStore attributes in a pyarrow.Table."""
    # Checks
    base_dir = check_is_tstore(base_dir)
    tstore_ids = check_tstore_ids(tstore_ids, base_dir=base_dir)
    # Define filters
    filters = get_tstore_ids_filters(tstore_ids=tstore_ids)
    # Retrieve filepath
    fpath = define_attributes_filepath(base_dir)
    # Read TStore attributes
    table = pq.read_table(
        fpath,
        use_pandas_metadata=True,
        filters=filters,
        filesystem=filesystem,
        use_threads=use_threads,
    )
    return table
