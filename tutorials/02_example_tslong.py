#!/usr/bin/env python3
"""
Created on Tue Jun 13 00:01:04 2023.

@author: ghiggi
"""

import tstore.tslong.pandas
import tstore.tslong.pyarrow

base_dir = "/tmp/dummy_tstore"
ts_variables = None
start_time = None
end_time = None
tstore_ids = None
use_threads = True
filesystem = None
columns = None


tslong_pyarrow = tstore.tslong.pyarrow.open_tslong(
    base_dir,
    ts_variables=ts_variables,
    start_time=start_time,
    end_time=end_time,
    tstore_ids=tstore_ids,
    columns=columns,
    filesystem=filesystem,
    use_threads=use_threads,
)

tslong_pyarrow.column_names


tslong_pandas = tstore.tslong.pandas.open_tslong(
    base_dir,
    ts_variables=ts_variables,
    start_time=start_time,
    end_time=end_time,
    tstore_ids=tstore_ids,
    columns=columns,
    filesystem=filesystem,
    use_threads=use_threads,
)

tslong_pandas
tslong_pandas.columns
