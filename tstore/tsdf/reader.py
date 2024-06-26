#!/usr/bin/env python3
"""
Created on Mon Jun 12 15:48:39 2023.

@author: ghiggi
"""

from pathlib import Path

import pandas as pd

from tstore.archive.io import get_ts_info
from tstore.tsdf.ts_class import TS
from tstore.tsdf.tsarray import TSArray


def _read_tsarray(base_dir: Path | str, ts_variable: str, var_prefix: str) -> pd.Series:
    """
    Read a TSArray into a pd.Series.

    Parameters
    ----------
    base_dir : path-like
        Base directory of the TStore.
    ts_variable : str
        Name of the time series variable.
    var_prefix : str
        Prefix for the variable directory in the TStore.

    Returns
    -------
    pd.Series
        TSArray Series.
    """
    # Retrieve TS fpaths and associated tstore_ids
    ts_fpaths, tstore_ids, partitions = get_ts_info(base_dir=base_dir, ts_variable=ts_variable, var_prefix=var_prefix)
    # Read TS objects
    # TODO: add option for TS format (dask, pandas, ...)
    list_ts = [TS.from_file(fpath, partitions=partitions) for fpath in ts_fpaths]

    # Create the TSArray Series
    tstore_ids = pd.Index(tstore_ids, dtype="string[pyarrow]", name="tstore_id")  # TODO: generalize
    ts_series = pd.Series(TSArray(list_ts), index=tstore_ids, name=ts_variable)
    return ts_series


def _read_tsarrays(base_dir: Path | str, metadata: dict, var_prefix: str) -> list[pd.Series]:
    """
    Read list of TSArrays.

    Parameters
    ----------
    base_dir : path-like
        Base directory of the TStore.
    metadata : dict-like
        Metadata dictionary.
    var_prefix : str
        Prefix for the variable directory in the TStore.

    Returns
    -------
    list of pd.Series
        List of TSArray Series.
    """
    ts_variables = metadata["ts_variables"]
    list_ts_series = [
        _read_tsarray(base_dir=base_dir, ts_variable=ts_variable, var_prefix=var_prefix) for ts_variable in ts_variables
    ]
    return list_ts_series
