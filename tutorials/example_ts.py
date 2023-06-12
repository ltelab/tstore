#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 11 20:56:07 2023

@author: ghiggi
"""
import pandas as pd 
import dask.datasets
import dask.dataframe as dd
from tstore.archive.ts import TS

 

df_dask = dask.datasets.timeseries(start='2000-01-01',
                                   end='2000-01-31', 
                                   freq='1s', 
                                   partition_freq='1d', 
                                   dtypes=None, 
                                   seed=None)

ts = TS(df_dask)

# Write to disk 
fpath = "/tmp/ts.parquet"
ts.to_disk(fpath)

# Read to disk 
ts = TS.from_file(fpath, partitioning_str="YYYY/MM")

# Get time series data
ts.data

# See TS methods 
dir(ts)

### 

 

df_pandas = df_dask.compute()
df_pandas


df = df_dask 

 