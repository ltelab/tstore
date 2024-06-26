#!/usr/bin/env python3
"""
Created on Mon Jun 12 15:49:54 2023.

@author: ghiggi
"""

from pathlib import Path

import numpy as np
import pandas as pd

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
    write_attributes(df=df_attributes, base_dir=base_dir)


def _write_ts_series(
    ts_series: pd.Series,
    base_dir: Path | str,
    tstore_structure: str,
    id_prefix: str,
    var_prefix: str,
) -> None:
    """
    Write TSDF TSArray.

    Parameters
    ----------
    ts_series : pd.Series of tstore.TS objects
        Series of TS objects.
    base_dir : path-like
        Base directory of the TStore.
    tstore_structure : ["id-var", "var-id"]
        TStore structure, either "id-var" or "var-id".
    id_prefix : str
        Prefix for the id directory in the TStore.
    var_prefix : str
        Prefix for the variable directory in the TStore.
    """
    ts_variable = ts_series.name
    tstore_ids = ts_series.index.array.astype(str)
    for tstore_id, ts in zip(tstore_ids, ts_series):
        if ts:  # not empty
            ts_fpath = define_tsarray_filepath(
                base_dir=base_dir,
                tstore_id=tstore_id,
                ts_variable=ts_variable,
                tstore_structure=tstore_structure,
                id_prefix=id_prefix,
                var_prefix=var_prefix,
            )
            ts.to_disk(ts_fpath)


def _write_tsarrays(
    df: pd.DataFrame,
    base_dir: Path | str,
    tstore_structure: str,
    id_prefix: str,
    var_prefix: str,
) -> None:
    """
    Write TSDF TSArrays.

    Parameters
    ----------
    df : pd.DataFrame
        Data frame with TSArray as columns.
    base_dir : path-like
        Base directory of the TStore.
    tstore_structure : ["id-var", "var-id"]
        TStore structure, either "id-var" or "var-id".
    id_prefix : str
        Prefix for the id directory in the TStore.
    var_prefix : str
        Prefix for the variable directory in the TStore.
    """
    tsarray_columns = _get_ts_variables(df)
    for column in tsarray_columns:
        _write_ts_series(
            ts_series=df[column],
            base_dir=base_dir,
            tstore_structure=tstore_structure,
            id_prefix=id_prefix,
            var_prefix=var_prefix,
        )


def _write_metadata(base_dir, tstore_structure, id_var, time_var, ts_variables, partitioning):
    """Write TStore metadata."""
    from tstore.archive.metadata.writers import write_tstore_metadata

    write_tstore_metadata(
        base_dir=base_dir,
        id_var=id_var,
        time_var=time_var,
        ts_variables=ts_variables,
        tstore_structure=tstore_structure,
        partitioning=partitioning,
    )


def write_tstore(
    df: pd.DataFrame,
    base_dir: Path | str,
    id_var: str,
    time_var: str,  # maybe not needed for TSDF?
    partitioning: str,
    tstore_structure: str,
    var_prefix: str,
    overwrite: bool,
) -> None:
    """
    Write TStore from TSDF object.

    Parameters
    ----------
    df : pd.DataFrame
        Data frame with TSArray as columns.
    base_dir : path-like
        Base directory of the TStore.
    id_var : str
        Name of the id variable.
    time_var : str
        Name of the time variable.
    partitioning : str
        Time partitioning string.
    tstore_structure : ["id-var", "var-id"], default "id-var"
        TStore structure, either "id-var" or "var-id".
    var_prefix : str
        Prefix for the variable directory in the TStore.
    overwrite : bool
        Overwrite existing TStore.
    """
    # Checks
    tstore_structure = check_tstore_structure(tstore_structure)
    base_dir = check_tstore_directory(base_dir, overwrite=overwrite)

    ts_variables = _get_ts_variables(df)
    partitioning = check_partitioning(partitioning, ts_variables=ts_variables)

    # id var
    if id_var is None:
        # if no `id_var` value is passed, the values are taken from the index.
        # TODO: enforce that index has a non-None name?
        id_var = df.index.name
    elif id_var not in df:
        # if a non-None `id_var` is passed but does not match a column of the data frame, take the values from the
        # index but take `id_var` as name
        df = df.reset_index(names=id_var)
    else:
        # if a non-None `id_var` is passed and matches a column of the data frame, there is nothing to do here
        pass

    # Write static attributes
    _write_attributes(df, base_dir=base_dir)

    # Write TSArrays
    _write_tsarrays(df, base_dir=base_dir, tstore_structure=tstore_structure, id_prefix=id_var, var_prefix=var_prefix)

    # Write TSArrays metadata
    ts_variables = _get_ts_variables(df)
    _write_metadata(
        base_dir=base_dir,
        tstore_structure=tstore_structure,
        ts_variables=ts_variables,
        id_var=id_var,
        time_var=time_var,
        partitioning=partitioning,
    )
