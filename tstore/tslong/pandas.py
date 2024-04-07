#!/usr/bin/env python3
"""
Created on Mon Jun 12 17:57:40 2023.

@author: ghiggi
"""
from tstore.tslong.pyarrow import open_tslong as pyarrow_reader


def open_tslong(
    base_dir,
    ts_variables=None,
    start_time=None,
    end_time=None,
    tstore_ids=None,
    columns=None,
    filesystem=None,
    use_threads=True,
):
    """Open TStore into long-format into pandas.DataFrame."""
    # Read exploiting pyarrow
    # - We use pyarrow to avoid pandas copies at concatenation and join operations !
    tslong_pyarrow = pyarrow_reader(
        base_dir,
        ts_variables=ts_variables,
        start_time=start_time,
        end_time=end_time,
        tstore_ids=tstore_ids,
        columns=columns,
        filesystem=filesystem,
        use_threads=use_threads,
    )

    # Conversion to pandas
    tslong_pandas = tslong_pyarrow.to_pandas()
    return tslong_pandas
