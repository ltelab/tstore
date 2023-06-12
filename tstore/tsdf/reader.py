#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 15:48:39 2023

@author: ghiggi
"""
import pandas as pd
from tstore.archive.ts import TS
from tstore.tsdf.extensions.array import TSArray
from tstore.archive.io import _get_ts_info
from tstore.archive.readers import (
    read_metadata,
    read_attributes,
)
from tstore.tsdf import TSDF


def _read_tsarray(base_dir, var, tstore_structure): 
    """Read a TSArray into a pd.Series."""        
    # Retrieve TS fpaths and associated identifiers
    ts_fpaths, identifiers = _get_ts_info(base_dir=base_dir, var=var, 
                                          tstore_structure=tstore_structure)
    # Read TS objects
    # TODO: add option for TS format (dask, pandas, ...)
    list_ts = [TS.from_file(fpath) for fpath in ts_fpaths]
    
    # Create the TSArray Series 
    identifiers = pd.Index(identifiers, dtype="string[pyarrow]", name='ID')
    ts_series = pd.Series(TSArray(list_ts), index=identifiers, name=var)
    return ts_series
    

def _read_tsarrays(base_dir, metadata):
    """Read list of TSArrays."""
    ts_columns = metadata["ts_variables"]
    tstore_structure = metadata["tstore_structure"]
    list_ts_series = [_read_tsarray(base_dir=base_dir, var=var,
                                    tstore_structure=tstore_structure) for var in ts_columns]
    return list_ts_series


def open_tsdf(base_dir): 
    """Open TSTORE into TSDF object."""
    # TODO: enable specify subset of TSArrays, attribute columns and rows to load
    
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
