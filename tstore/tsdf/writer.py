#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
from tstore.tsdf.extensions.ts_dtype import TSDtype


def _get_tsarray_columns(df):
    """Get list of TSArray columns."""
    columns = np.array(list(df.columns))
    tsarray_columns = columns[
        [isinstance(df[column].dtype, TSDtype) for column in columns]
    ].tolist()
    return tsarray_columns


def _get_static_columns(df):
    """Get list of non-TSArray columns."""
    columns = list(df.columns)
    tsarray_columns = _get_tsarray_columns(df)
    static_columns = list(set(columns).symmetric_difference(tsarray_columns))
    return static_columns


def _write_attributes(df, base_dir):
    """Write TSDF static attributes."""
    from tstore.archive.writers import write_attributes

    static_columns = _get_static_columns(df)
    write_attributes(df=df[static_columns], base_dir=base_dir)


def _write_ts_series(ts_series, base_dir, tstore_structure):
    """Write TSDF TSArray."""
    var = ts_series.name
    identifiers = ts_series.index.array.astype(str)
    for identifier, ts in zip(identifiers, ts_series):
        if ts:  # not empty
            ts_fpath = define_tsarray_filepath(
                base_dir=base_dir,
                identifier=identifier,
                var=var,
                tstore_structure=tstore_structure,
            )
            ts.to_disk(ts_fpath)


def _write_tsarrays(df, base_dir, tstore_structure):
    """Write TSDF TSArrays."""
    tsarray_columns = _get_tsarray_columns(df)
    for column in tsarray_columns:
        _write_ts_series(
            ts_series=df[column], base_dir=base_dir, tstore_structure=tstore_structure
        )


def _write_metadata(df, base_dir, tstore_structure):
    """Write TSTORE metadata."""
    from tstore.archive.writers import write_metadata

    ts_variables = _get_tsarray_columns(df)
    write_metadata(
        base_dir=base_dir, ts_variables=ts_variables, tstore_structure=tstore_structure
    )


def write_tstore(
    df, base_dir, partition_str=None, tstore_structure="id-var", overwrite=True
):
    """Write TSTORE from TSDF object."""
    # Checks
    tstore_structure = check_tstore_structure(tstore_structure)
    base_dir = check_tstore_directory(base_dir, overwrite=overwrite)

    # Write static attributes
    _write_attributes(df, base_dir=base_dir)

    # Write TSArrays
    _write_tsarrays(df, base_dir=base_dir, tstore_structure=tstore_structure)

    # Write TSArrays metadata
    _write_metadata(df, base_dir=base_dir, tstore_structure=tstore_structure)
