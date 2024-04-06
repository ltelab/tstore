#!/usr/bin/env python3
"""
Created on Mon Jun 12 23:22:02 2023.

@author: ghiggi
"""
import pandas as pd

from tstore.archive.checks import check_is_tstore, check_tstore_ids
from tstore.archive.io import define_attributes_filepath


def read_attributes(base_dir, tstore_ids=None):
    """Read TStore attributes in a pandas.DataFrame."""
    # Checks
    base_dir = check_is_tstore(base_dir)
    tstore_ids = check_tstore_ids(tstore_ids, base_dir=base_dir)
    # Retrieve filepath
    fpath = define_attributes_filepath(base_dir)
    # Read TStore attributes
    df = pd.read_parquet(fpath, engine="pyarrow", dtype_backend="pyarrow")
    df.index = df.index.astype("string[pyarrow]")
    # Subset tstore_ids
    # TODO: maybe possible to pass filters to pd.read_parquet
    return df
