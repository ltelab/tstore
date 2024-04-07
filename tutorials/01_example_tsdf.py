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
df_series.values  # TSArray # noqa
df_series.array  # TSArray # zero-copy reference to the data !
df_series.to_numpy()  # object dtype

# Concatenation i.e works
# --> TODO: check copy() !!!
pd.concat((df_series, df_series))

# Join to df_attrs
ts_variable = "precipitation"
df_attrs[ts_variable] = df_series

# Create TSDF
tsdf = TSDF(df_attrs)
dir(tsdf)

# Write TStore
base_dir = "/tmp/dummy_tstore"
partition_str = None  # TO IMPLEMENT
tstore_structure = "id-var"
overwrite = True
tsdf.to_tstore(
    base_dir,
    partition_str=partition_str,
    tstore_structure=tstore_structure,
    overwrite=overwrite,
)

# -----------------------------------------------------------------------------.
# Open TSDF
tsdf = tstore.open_tsdf(base_dir)

# -----------------------------------------------------------------------------.
# Open TSLONG
# --> Open with pyarrow for zero copy at concatenation
# --> Conversion to pandas, dask, polars at the end
tslong = tstore.open_tslong(base_dir, ts_variables=["precipitation"])


# Open TSWIDE
tswide = tstore.open_tswide(base_dir, ts_variable="precipitation", ts_subset="name")


# Open TSDT
# --> DataTree

# -----------------------------------------------------------------------------.
# Write TSLONG


####--------------------------------------------------------------------------.
