#!/usr/bin/env python3
"""
Created on Sun Apr  7 21:10:05 2024.

@author: ghiggi
"""
# ruff: noqa: E402

import dask.datasets
import numpy as np
import pandas as pd

import tstore

#### Create data

# Define tstore_ids
tstore_ids = np.arange(1, 5)

# Create Pandas DFLong
list_df = []
for tstore_id in tstore_ids:
    df_dask = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-02",
        # end="2000-01-31",
        freq="1s",
        partition_freq="1d",
        dtypes=None,
        seed=None,
    )
    df = df_dask.compute()
    df["store_id"] = tstore_id
    list_df.append(df)


df = pd.concat(list_df)
df["static_var"] = df["store_id"].copy()


df.shape  # 345_600

base_dir = "/tmp/dummy_tstore"
tstore_structure = "id-var"
overwrite = True
id_var = "store_id"
time_var = "time"
static_variables = ["static_var"]
geometry = None  # NOT IMPLEMENTED YET

# Same partitioning for all TS
partitioning = "year/month"
# Partitioning specific to each TS
partitioning = {"precipitation": "year/month"}

# Each timeseries is a TS object
ts_variables = ["name", "id", "x", "y"]
# Group multiple timeseries into one TS object
ts_variables = {"precipitation": ["name", "id", "x", "y"]}

tslong = tstore.TSLong.wrap(df)
tslong.to_tstore(
    # TSTORE options
    base_dir,
    # DFLONG attributes
    id_var=id_var,
    time_var=time_var,
    ts_variables=ts_variables,
    static_variables=static_variables,
    # TSTORE options
    partitioning=partitioning,
    tstore_structure=tstore_structure,
    overwrite=overwrite,
)

####--------------------------------------------------------------------.
#### Load TSTORE as TSDF
# - TSDF.from_store(base_dir, backend="pandas")
# - tstore.open_tsdf(base_dir, backend="pandas")

# Load as TSDF.from_store(base_dir)
tsdf1 = tstore.open_tsdf(base_dir)

####--------------------------------------------------------------------.
#### Load TSTORE as PANDAS TSLONG
# - TSLONG.from_store(base_dir, backend="pandas")
# - tstore.open_tslong(base_dir, backend="pandas")

# Load as TSLONG in pandas
tslong = tstore.open_tslong(base_dir, ts_variables=["precipitation"])
tslong.shape  # 345_600
tslong

####--------------------------------------------------------------------.
#### Load TSTORE as POLARS TSLONG
tslong_pl = tstore.open_tslong(base_dir, backend="polars", ts_variables=["precipitation"])

tslong_pl

####--------------------------------------------------------------------.
### POLARS TSLONG.to_tstore()
base_dir = "/tmp/dummy_tstore_polars"
tstore_structure = "id-var"
overwrite = True
id_var = "store_id"
time_var = "time"
static_variables = ["static_var"]
geometry = None  # NOT IMPLEMENTED YET

# Same partitioning for all TS
partitioning = "year/month"
# Partitioning specific to each TS
partitioning = {"precipitation": "year/month"}

# Each timeseries is a TS object
ts_variables = ["name", "id", "x", "y"]
# Group multiple timeseries into one TS object
ts_variables = {"precipitation": ["name", "id", "x", "y"]}

tslong_pl.to_tstore(
    # TSTORE options
    base_dir,
    # DFLONG attributes
    id_var=id_var,
    time_var=time_var,
    ts_variables=ts_variables,
    static_variables=static_variables,
    # TSTORE options
    partitioning=partitioning,
    tstore_structure=tstore_structure,
    overwrite=overwrite,
)


# ####--------------------------------------------------------------------.
# # Check equality
tslong_pl1 = tstore.open_tslong(base_dir, backend="polars", ts_variables=["precipitation"])

# Currently the row order varies !
tslong_pl
tslong_pl1

# ####--------------------------------------------------------------------.
# ts_variables=None
# ts_variable="precipitation"
# start_time=None
# end_time=None
# columns=None
# filesystem=None
# use_threads=True

# fpath = fpaths[0]
