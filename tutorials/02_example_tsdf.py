#!/usr/bin/env python3
"""
Created on Sun Jun 11 22:21:38 2023.

@author: ghiggi
"""

import dask.datasets
import numpy as np
import pandas as pd

import tstore
from tstore import (
    TS,
    TSDF,
    TSArray,
)

id_var = "tstore_id"  # should be required ... avoid id_var to be index !
time_var = "time"  # should be retrieved from TS.time_var in TSDF

# Define tstore_ids
tstore_ids = np.arange(1, 5)

# Define dataframe with attributes
data = {"var1": ["A", "B", "C", "D"], "var2": [1.0, 2.0, 3.0, 4.0]}
df_attrs = pd.DataFrame(data, index=tstore_ids)


# Create TSArray
ts_dict = {}
for tstore_id in tstore_ids:
    df_dask = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-31",
        freq="1s",
        partition_freq="1d",
        dtypes=None,
        seed=None,
    )
    ts_dict[tstore_id] = TS(df_dask)


ts_dict

# Create TSArray
data = list(ts_dict.values())
ts_arr = TSArray(data)
print(ts_arr)
repr(ts_arr)

# Create Series with TS Dtype
df_series = pd.Series(ts_arr, index=tstore_ids)
df_series  # dtype: TS[dask.DataFrame]  ...  pandas/polars/pyarrow
df_series.dtype  # TSDtype
df_series.values  # TSArray
df_series.array  # TSArray # zero-copy reference to the data !
df_series.to_numpy()  # object dtype

# Concatenation i.e works
# --> TODO: check copy() !!!
pd.concat((df_series, df_series))

# Join to df_attrs
ts_variable = "precipitation"
df_attrs[ts_variable] = df_series
df_attrs[id_var] = [1, 2, 3, 4]
df_attrs[id_var] = df_attrs[id_var].astype("large_string[pyarrow]")  # THIS IS REQUIRED TO MAKE JOIN/MERGE TO WORKS !

#### TSDF - Creation
# --> Note: currently defined in tstore.tsdf.__init__ ... to be moved !

tsdf = TSDF(df_attrs)
type(tsdf)
dir(tsdf)

# Add TSArray
tsdf["precipitation2"] = df_series
tsdf["precipitation2"]

# Remove TSArray
tsdf1 = tsdf.drop(columns="precipitation2")
tsdf1
type(tsdf1)  # WARNING: this returns a pd.DataFrame !

# Subsetting
tsdf1 = tsdf.iloc[0:10]
tsdf1
type(tsdf1)  # WARNING: this returns a pd.DataFrame !

# Join/merge methods
tsdf1 = tsdf.join(tsdf.iloc[0])
tsdf1
type(tsdf1)  # WARNING: this returns a pd.DataFrame --> should be TSDF ... must be wrapped !

# Remove unsupported methods (or just add the one supported ?)
# tsdf.min()

# If class inherit pandas.DataFrame or geopandas.DataFrame
# --> Need to redefine methods to returns TSDF (similar concept applies to TSLONG, ...)
# Or we avoid inheritance from *.DataFrame and we re-implement the relevant methods calling internally the
# *.DataFrame.<methods>


#### TSDF - Write TStore
base_dir = "/tmp/dummy_tstore_from_tsdf"
partitioning = None  # TO IMPLEMENT
tstore_structure = "id-var"
overwrite = True
tsdf.to_tstore(
    base_dir,
    id_var=id_var,
    time_var=time_var,
    partitioning=partitioning,
    tstore_structure=tstore_structure,
    overwrite=overwrite,
)


# -----------------------------------------------------------------------------.
#### Open TSDF
tsdf1 = tstore.open_tsdf(base_dir)

# -----------------------------------------------------------------------------.
#### Open TSLONG
# --> Open with pyarrow for zero copy at concatenation
# --> Conversion to pandas, dask, polars at the end

tslong = tstore.open_tslong(base_dir, ts_variables=["precipitation"])
tslong

####--------------------------------------------------------------------------.
#### Open TSWIDE

# tswide = tstore.open_tswide(base_dir, ts_variable="precipitation", ts_subset="name")

####--------------------------------------------------------------------------.
# Open TSDT (datatree/xvec)


####--------------------------------------------------------------------------.
#### INTERFACE OPTIONS

# open_store(format="tslong", backend="pandas")
# open_store(format="tsdf", backend="pandas")
# open_store(format="tswide", backend="pandas")

# TSDF.open(backend="pandas")
# TSDF.from_store(backend="pandas")

# TSLONG.open(backend="pandas")
# TSLONG.from_store(backend="pandas")

####--------------------------------------------------------------------------.
#### POINTS TO CONSIDER
# - When we will implement apply() functions, if the function reduce the data we
#   could loose the time column.
# - Option 1: Reduce to multiple values (i.e. month indices / season indices, no datetime column)
#   Example:
#   - Computing the monthly mean over all series would produce a dataframe with a column
#     with the month_name/number and columns with the monthly statistics
#   - Such type of output will requires another pandas type (i.e. ListArray) and
#     specific writer options (partitions=None, no time_var)
# - Option 2: Reduce to single value  (no time info anymore)
#   Example: Long-term mean/variance of a time series
#   - The output could be directly attached to the pandas dataframe (as a static column)


####--------------------------------------------------------------------------.
#### IMPLEMENTATIONS / THOUGHTS
# - Arrow/Pyarrow allows for zero-copy conversion between pandas/(dask)/polars. We should exploit that !
# - Pyarrow.Table object is difficult to manipulate (I would use it only for conversion across backend and
#   reading/writing to/from disk)
# - ID should be always stored as string (pyarrow.large_string), so to have some dtype when doing join/merge operations
# - I would suggest to first start implement the I/O and conversion with pandas and polars (with data in memory) (and
#   eventually dask dataframe)
# - When this is set (and eventually tested), we can implement the lazy reading of TSDF
#   - dask.delayed for pandas and polars backends
#   - dask.dataframe for dask backend
#   - lazy polars for lazy_polars backend ...
# - I would expect that we enable to define a series of lazy operations (which can change the backend of the TS during
#   computations), we must have something to control / enforce the final backend of the data to be written to disk.
#   Because the dask delayed object does not know what will be the dtype of the TS.data
# - A tool to switch TS object from an backend to another should be implemented
# - A method to .compute() data in memory should be also implemented

# - Ideally one should be able to apply functions to many TSArray etc, and the actual execution
#  only taking place when writing to disk the timeseries (id per id),  loading data into memory only at that moment and
#  freeing the memory when the id is processed.

####--------------------------------------------------------------------------.
