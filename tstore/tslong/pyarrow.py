#!/usr/bin/env python3
"""
Created on Mon Jun 12 17:57:14 2023.

@author: ghiggi
"""
from functools import reduce

import numpy as np
import pyarrow as pa

from tstore.archive.attributes.pyarrow import read_attributes
from tstore.archive.checks import (
    check_is_tstore,
    check_start_end_time,
    check_ts_variables,
    check_tstore_ids,
)
from tstore.archive.io import get_ts_info
from tstore.archive.ts.pyarrow import open_ts


def _read_ts(
    fpath,
    tstore_id,
    start_time=None,
    end_time=None,
    columns=None,
    filesystem=None,
    use_threads=True,
):
    """Read a single TS in pyarrow long-format."""
    # Read TS in pyarrow long format
    table = open_ts(
        fpath=fpath,
        start_time=start_time,
        end_time=end_time,
        columns=columns,
        filesystem=filesystem,
        use_threads=use_threads,
    )
    # Add tstore_id
    tstore_id = str(tstore_id)
    tstore_id = pa.array(np.repeat(tstore_id, len(table)), type=pa.string())
    table = table.add_column(0, "tstore_id", tstore_id)
    return table


def _read_ts_variable(
    base_dir,
    ts_variable,
    start_time=None,
    end_time=None,
    columns=None,
    filesystem=None,
    use_threads=True,
):
    """Read a TStore ts_variable into pyarrow long-format."""
    # Find TS and associated TStore IDs
    fpaths, tstore_ids = get_ts_info(base_dir=base_dir, ts_variable=ts_variable)
    # Read each TS
    list_tables = [
        _read_ts(
            fpath=fpath,
            tstore_id=tstore_id,
            start_time=start_time,
            end_time=end_time,
            columns=columns,
            filesystem=filesystem,
            use_threads=use_threads,
        )
        for fpath, tstore_id in zip(fpaths, tstore_ids)
    ]
    # Concatenate the tables
    table = pa.concat_tables(list_tables)
    return table


def _join_tables(left_table, right_table):
    """Joining functions of pyarrow tables."""
    # TODO: update keys to 'time'
    return left_table.join(
        right_table,
        keys=["timestamp", "tstore_id"],
        join_type="full outer",
    )


def _read_ts_variables(
    base_dir,
    ts_variables,
    start_time=None,
    end_time=None,
    columns=None,
    filesystem=None,
    use_threads=True,
):
    """Read TStore ts_variables into pyarrow long-format."""
    # TODO: columns must be a dictionary {ts_variable: [...]}

    # Read TS of all ts_variables in long-format
    list_tables = [
        _read_ts_variable(
            base_dir=base_dir,
            ts_variable=ts_variable,
            start_time=start_time,
            end_time=end_time,
            columns=columns,  # columns[ts_variable] in future
            filesystem=filesystem,
            use_threads=use_threads,
        )
        for ts_variable in ts_variables
    ]

    # Check that each table has different column names
    # --> Except from 'time' and 'tstore_id' on which align !
    # TODO:

    # Check each table has 'time' and 'tstore_id'
    # TODO:

    # Iteratively join the tables
    table = reduce(_join_tables, list_tables)

    return table


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
    """Open TStore into long-format into pyarrow.Table."""
    # TODO: columns must be a dictionary {ts_variable: [...]}
    # --> Preprocess in dictionary {ts_variable: None ... }

    # Checks
    base_dir = check_is_tstore(base_dir)
    ts_variables = check_ts_variables(ts_variables, base_dir=base_dir)
    tstore_ids = check_tstore_ids(tstore_ids, base_dir=base_dir)
    start_time, end_time = check_start_end_time(start_time, end_time)

    # Get list of tslong for each ts_variable
    table = _read_ts_variables(
        base_dir=base_dir,
        ts_variables=ts_variables,
        start_time=start_time,
        end_time=end_time,
        columns=columns,
        filesystem=filesystem,
        use_threads=use_threads,
    )

    # Read TStore attributes
    table_attrs = read_attributes(
        base_dir=base_dir,
        tstore_ids=tstore_ids,
        filesystem=filesystem,
        use_threads=use_threads,
    )
    # Join (duplicate) table_attrs on table
    tslong = table.join(table_attrs, keys=["tstore_id"], join_type="full outer")

    # Return tslong
    # TODO: add class ?
    return tslong
