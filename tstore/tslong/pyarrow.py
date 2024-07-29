"""Module defining the TSLongPyArrow wrapper."""

from functools import reduce
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa

from tstore.archive.attributes.pyarrow import read_attributes
from tstore.archive.checks import (
    check_is_tstore,
    check_start_end_time,
    check_ts_variables,
    check_tstore_ids,
)
from tstore.archive.io import get_geometry_var, get_id_var, get_ts_info
from tstore.archive.ts.readers.pyarrow import open_ts
from tstore.tslong.tslong import TSLong

if TYPE_CHECKING:
    # To avoid circular imports
    pass


class TSLongPyArrow(TSLong):
    """Wrapper for a long-form PyArrow timeseries dataframe."""

    def to_tstore(
        self,
        base_dir,
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,
    ):
        """Write the wrapped dataframe as a TStore structure."""
        pandas_tslong = self.change_backend(new_backend="pandas")
        pandas_tslong.to_tstore(
            base_dir=base_dir,
            partitioning=partitioning,
            tstore_structure=tstore_structure,
            overwrite=overwrite,
        )

    @staticmethod
    def from_tstore(
        base_dir,
        ts_variables=None,
        start_time=None,
        end_time=None,
        tstore_ids=None,
        columns=None,
        filesystem=None,
        use_threads=True,
    ) -> "TSLongPyArrow":
        """Open a TStore file structure as a TSLongPyArrow wrapper around a Pandas long dataframe."""
        # Checks
        base_dir = check_is_tstore(base_dir)
        ts_variables = check_ts_variables(ts_variables, base_dir=base_dir)
        tstore_ids = check_tstore_ids(tstore_ids, base_dir=base_dir)
        start_time, end_time = check_start_end_time(start_time, end_time)
        id_var = get_id_var(base_dir)
        time_var = "time"
        geometry_var = get_geometry_var(base_dir)

        # Get list of tslong for each ts_variable
        table, ts_variables_dict = _read_ts_variables(
            base_dir=base_dir,
            id_var=id_var,
            time_var=time_var,
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
        static_vars = table_attrs.schema.names
        static_vars.remove(id_var)

        # Join (duplicate) table_attrs on table
        tslong = table.join(table_attrs, keys=[id_var], join_type="full outer")

        return TSLongPyArrow(
            tslong,
            id_var=id_var,
            time_var=time_var,
            ts_vars=ts_variables_dict,
            static_vars=static_vars,
            geometry_var=geometry_var,
        )


def _read_ts(
    fpath,
    tstore_id,
    partitions,
    id_var,
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
        partitions=partitions,
        start_time=start_time,
        end_time=end_time,
        columns=columns,
        filesystem=filesystem,
        use_threads=use_threads,
    )
    # Add tstore_id (as large_string dtype to avoid join errors)
    tstore_id = str(tstore_id)
    tstore_id = pa.array(np.repeat(tstore_id, len(table)), type=pa.string()).cast(pa.large_string())
    table = table.add_column(0, id_var, tstore_id)
    return table


def _read_ts_variable(
    base_dir,
    id_var,
    ts_variable,
    start_time=None,
    end_time=None,
    columns=None,
    filesystem=None,
    use_threads=True,
):
    """Read a TStore ts_variable into pyarrow long-format."""
    # Find TS and associated TStore IDs
    fpaths, tstore_ids, partitions = get_ts_info(base_dir=base_dir, ts_variable=ts_variable)
    # Read each TS
    list_tables = [
        _read_ts(
            fpath=fpath,
            partitions=partitions,
            tstore_id=tstore_id,
            id_var=id_var,
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


def _join_tables(left_table, right_table, id_var, time_var):
    """Joining functions of pyarrow tables."""
    return left_table.join(
        right_table,
        keys=[time_var, id_var],
        join_type="full outer",
    )


def _read_ts_variables(
    base_dir,
    id_var,
    time_var,
    ts_variables,
    start_time=None,
    end_time=None,
    columns=None,
    filesystem=None,
    use_threads=True,
) -> tuple[pa.Table, dict[str, list[str]]]:
    """Read TStore ts_variables into pyarrow long-format."""
    # Read TS of all ts_variables in long-format
    list_tables = [
        _read_ts_variable(
            base_dir=base_dir,
            id_var=id_var,
            ts_variable=ts_variable,
            start_time=start_time,
            end_time=end_time,
            columns=columns,  # columns[ts_variable] in future
            filesystem=filesystem,
            use_threads=use_threads,
        )
        for ts_variable in ts_variables
    ]

    # Check each table has 'time' and 'tstore_id'
    ts_variables_dict = {}
    for ts_variable, table in zip(ts_variables, list_tables):
        columns = table.schema.names

        if id_var not in columns:
            raise ValueError(f"ID variable '{id_var}' not found in '{ts_variable}' table.")

        if time_var not in columns:
            raise ValueError(f"Time variable '{time_var}' not found in '{ts_variable}' table.")

        columns.remove(id_var)
        columns.remove(time_var)

        ts_variables_dict[ts_variable] = columns

    # Check that each table has different column names
    # --> Except from 'time' and 'tstore_id' on which align !
    # TODO:

    # Iteratively join the tables
    table = reduce(lambda left, right: _join_tables(left, right, id_var=id_var, time_var=time_var), list_tables)

    return table, ts_variables_dict
