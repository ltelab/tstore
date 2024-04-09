#!/usr/bin/env python3
"""
Created on Sun Jun 11 20:56:07 2023.

@author: ghiggi
"""
import dask.datasets
import pyarrow as pa
import pandas as pd
import polars as pl
from tstore import TS

####-------------------------------------.
#### Create toy timeseries (dataframe)
df_dask = dask.datasets.timeseries(
    start="2000-01-01",
    end="2000-01-31",
    freq="1s",
    partition_freq="1d",
    dtypes=None,
    seed=None,
)
df_pd = df_dask.compute()
df_arrow = pa.Table.from_pandas(df_pd)
df_pd = df_arrow.to_pandas(types_mapper=pd.ArrowDtype)  # To have pandas with arrow dtypes
df_pl = pl.from_arrow(df_arrow)

####-------------------------------------.
#### Create TS object 
# - This currently works only for pandas and dask dataframe (with time as index)
# - Maybe we should ensure 'time' is a column for cross-compatibility (no index in polars and arrow)
ts_pd = TS(df_pd)
ts_dask = TS(df_dask)

# Get time series data
ts_pd.data
ts_dask.data
ts_dask.data.compute()

# See TS methods
dir(ts_pd)
dir(ts_dask)

# Not working yet 
# ts_pl = TS(df_pl)
# ts = TS(df_arrow) # maybe arrow should just be used as backend for I/O (diffucult to manipulate !)

####-------------------------------------.
#### Write TS object to disk 
# - This currently use old code designed for dask series/dataframe
# - Should be based on tstore.archive.ts.writers 
# --> All code should likely exploit the pyarrow write_partitioned_dataset() function
# --> Maybe we should call the method TS.to_parquet() 
# --> Also when writing TSLONG to TStore, we could create a TS to then write to disk (see comment in tstore.tslong.pandas/polars)

# Write to disk
fpath = "/tmp/ts_dask.parquet"
ts_dask.to_disk(fpath)

# Not working 
# fpath = "/tmp/ts_pd.parquet"
# ts_pd.to_disk(fpath)

# fpath = "/tmp/ts_pl.parquet"
# ts_pl.to_disk(fpath)

####-------------------------------------.
#### Read TS from disk 
# --> Pandas, Polars and pyarrow <read_parquet> functions loads data into memory !  
# --> Should we exploit only the pyarrow read function and then convert to the backend of choice? 
# --> Should we wrap the reading with dask.delayed to enable lazy reading (not actually read data into memory till needed ?) 
# --> Dask (dd.read_parquet) and LazyPolars (scan_parquet) functions allow direct lazy reads 

# Dask code 
ts = TS.from_file(fpath, partitions=[]) # logic of partitions not implemented ... 
ts.data

 

####-------------------------------------.
# Brainstorm and TODOs
# - TS class must be refactored and implemented for various backend 
# - Time column should be fixed to 'time' or do we define/use the TS.time_var attribute?
# --> What if in TSDF 'time' column name varies between TS? Should we enforce one across TSTORE ... `

####-------------------------------------.