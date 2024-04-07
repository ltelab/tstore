#!/usr/bin/env python3
"""
Created on Mon Jun 12 15:48:39 2023.

@author: ghiggi
"""
import pandas as pd

from tstore.archive.io import get_ts_info
from tstore.archive.readers import read_metadata
from tstore.tsdf import TSDF
from tstore.tsdf.extensions.array import TSArray
from tstore.tsdf.ts_class import TS


def _read_tsarray(base_dir, ts_variable):
    """Read a TSArray into a pd.Series."""
    # Retrieve TS fpaths and associated tstore_ids
    ts_fpaths, tstore_ids = get_ts_info(base_dir=base_dir, ts_variable=ts_variable)
    # Read TS objects
    # TODO: add option for TS format (dask, pandas, ...)
    list_ts = [TS.from_file(fpath) for fpath in ts_fpaths]

    # Create the TSArray Series
    tstore_ids = pd.Index(tstore_ids, dtype="string[pyarrow]", name="tstore_id")  # TODO: generalize
    ts_series = pd.Series(TSArray(list_ts), index=tstore_ids, name=ts_variable)
    return ts_series


def _read_tsarrays(base_dir, metadata):
    """Read list of TSArrays."""
    ts_variables = metadata["ts_variables"]
    list_ts_series = [_read_tsarray(base_dir=base_dir, ts_variable=ts_variable) for ts_variable in ts_variables]
    return list_ts_series


def open_tsdf(base_dir):
    """Open TStore into TSDF object."""
    # TODO: enable specify subset of TSArrays, attribute columns and rows to load
    # TODO: read_attributes using geopandas --> geoparquet
    # TODO: separate TSDF class if geoparquet (TSDF inherit from geopandas.GeoDataFrame ?)
    from tstore.archive.attributes.pandas import read_attributes

    # Read TStore metadata
    metadata = read_metadata(base_dir=base_dir)

    # Read TStore attributes
    df = read_attributes(base_dir)

    # Get list of TSArrays
    list_ts_series = _read_tsarrays(base_dir, metadata)

    # Join TSArrays to dataframe
    for ts_series in list_ts_series:
        df = df.join(ts_series, how="left")
        #  pd.merge(df_attrs, df_series, left_index=True, right_index=True)

    # Return the TSDF
    return TSDF(df)
