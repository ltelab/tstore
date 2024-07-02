#!/usr/bin/env python3
"""
Created on Mon Jun 12 15:49:54 2023.

@author: ghiggi
"""

import numpy as np

from tstore.archive.io import (
    check_tstore_directory,
    check_tstore_structure,
    define_tsarray_filepath,
)
from tstore.archive.partitions import check_partitioning
from tstore.tsdf.ts_dtype import TSDtype


def _get_ts_variables(df):
    """Get list of TSArray columns."""
    columns = np.array(list(df.columns))
    ts_variables = columns[[isinstance(df[column].dtype, TSDtype) for column in columns]].tolist()
    return ts_variables


def _get_static_columns(df):
    """Get list of non-TSArray columns."""
    columns = list(df.columns)
    ts_variables = _get_ts_variables(df)
    static_columns = list(set(columns).symmetric_difference(ts_variables))
    return static_columns


def _write_attributes(df, base_dir):
    """Write TSDF static attributes."""
    from tstore.archive.attributes.pandas import write_attributes

    static_columns = _get_static_columns(df)
    df_attributes = df[static_columns]
    df_attributes.index.name = "tstore_id"
    write_attributes(df=df_attributes, base_dir=base_dir)


def _write_ts_series(ts_series, tstore_ids, base_dir, tstore_structure):
    """Write TSDF TSArray."""
    ts_variable = ts_series.name
    for tstore_id, ts in zip(tstore_ids, ts_series):
        if ts:  # not empty
            ts_fpath = define_tsarray_filepath(
                base_dir=base_dir,
                tstore_id=tstore_id,
                ts_variable=ts_variable,
                tstore_structure=tstore_structure,
            )
            ts.to_disk(ts_fpath)


def _write_tsarrays(df, id_var, base_dir, tstore_structure):
    """Write TSDF TSArrays."""
    tsarray_columns = _get_ts_variables(df)
    for column in tsarray_columns:
        _write_ts_series(
            ts_series=df[column],
            tstore_ids=df[id_var],
            base_dir=base_dir,
            tstore_structure=tstore_structure,
        )


def _write_metadata(base_dir, tstore_structure, id_var, ts_variables, partitioning):
    """Write TStore metadata."""
    from tstore.archive.metadata.writers import write_tstore_metadata

    write_tstore_metadata(
        base_dir=base_dir,
        id_var=id_var,
        ts_variables=ts_variables,
        tstore_structure=tstore_structure,
        partitioning=partitioning,
    )


def write_tstore(
    df,
    base_dir,
    id_var,
    partitioning,
    tstore_structure="id-var",
    overwrite=True,
):
    """Write TStore from TSDF object."""
    # Checks
    tstore_structure = check_tstore_structure(tstore_structure)
    base_dir = check_tstore_directory(base_dir, overwrite=overwrite)

    ts_variables = _get_ts_variables(df)
    partitioning = check_partitioning(partitioning, ts_variables=ts_variables)

    # Write static attributes
    _write_attributes(df, base_dir=base_dir)

    # Write TSArrays
    _write_tsarrays(df, id_var, base_dir=base_dir, tstore_structure=tstore_structure)

    # Write TSArrays metadata
    ts_variables = _get_ts_variables(df)
    _write_metadata(
        base_dir=base_dir,
        tstore_structure=tstore_structure,
        ts_variables=ts_variables,
        id_var=id_var,
        partitioning=partitioning,
    )
