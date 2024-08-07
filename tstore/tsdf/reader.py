#!/usr/bin/env python3
"""
Created on Mon Jun 12 15:48:39 2023.

@author: ghiggi
"""

import pandas as pd

from tstore.archive.io import get_ts_info
from tstore.tsdf.ts_class import TS
from tstore.tsdf.tsarray import TSArray


def _read_tsarray(base_dir, ts_variable):
    """Read a TSArray into a pd.Series."""
    # Retrieve TS fpaths and associated tstore_ids
    ts_fpaths, tstore_ids, partitions = get_ts_info(base_dir=base_dir, ts_variable=ts_variable)
    # Read TS objects
    # TODO: add option for TS format (dask, pandas, ...)
    list_ts = [TS.from_disk(fpath, partitions=partitions) for fpath in ts_fpaths]

    # Create the TSArray Series
    tstore_ids = pd.Index(tstore_ids, dtype="string[pyarrow]", name="tstore_id")  # TODO: generalize
    ts_series = pd.Series(TSArray(list_ts), index=tstore_ids, name=ts_variable)
    return ts_series


def _read_tsarrays(base_dir, metadata):
    """Read list of TSArrays."""
    ts_variables = metadata["ts_variables"]
    list_ts_series = [_read_tsarray(base_dir=base_dir, ts_variable=ts_variable) for ts_variable in ts_variables]
    return list_ts_series
