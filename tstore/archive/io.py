"""TStore I/O tools."""

import glob
import os
import shutil
from pathlib import Path


def check_tstore_structure(tstore_structure):
    """Check validity of TStore structure."""
    if tstore_structure not in ["id-var", "var-id"]:
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    return tstore_structure


def check_tstore_directory(base_dir, overwrite):
    """Check TStore directory."""
    # Check if exists, remove or raise error
    if os.path.exists(base_dir):
        if overwrite:
            shutil.rmtree(base_dir)
        else:
            raise ValueError("TStore already existing at {base_dir}")
    # Create directory
    os.makedirs(base_dir, exist_ok=True)
    return base_dir


def define_attributes_filepath(base_dir):
    """Define filepath of TStore attributes."""
    fpath = os.path.join(base_dir, "_attributes.parquet")
    return fpath


def define_tsarray_filepath(
    base_dir: Path | str,
    tstore_id: str,
    ts_variable: str,
    tstore_structure: str,
    id_prefix: str,
    var_prefix: str,
) -> str:
    """
    Define filepath of a TStore TS.

    Parameters
    ----------
    base_dir : path-like
        Base directory of the TStore.
    tstore_id : str
        Value of the time series ID.
    ts_variable : str
        Name of the time series variable.
    ts_structure : ["id-var", "var-id"]
        TStore structure, either "id-var" or "var-id".
    id_prefix : str
        Prefix for the ID directory in the TStore.
    var_prefix : str
        Prefix for the variable directory in the TStore.

    Returns
    -------
    fpath : str
        Filepath for the time series.
    """
    id_dir_basename = f"{id_prefix}={tstore_id}"
    var_dir_basename = f"{var_prefix}={ts_variable}"
    if tstore_structure == "id-var":
        fpath = os.path.join(base_dir, id_dir_basename, var_dir_basename)
    elif tstore_structure == "var-id":
        fpath = os.path.join(base_dir, var_dir_basename, id_dir_basename)
    else:
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    return fpath


def define_metadata_filepath(base_dir):
    """Define TStore metadata filepath."""
    metadata_fpath = os.path.join(base_dir, "tstore_metadata.yaml")
    return metadata_fpath


def get_tstore_structure(base_dir):
    """Get TStore structure."""
    from tstore.archive.metadata.readers import read_tstore_metadata

    metadata = read_tstore_metadata(base_dir=base_dir)
    return metadata["tstore_structure"]


def get_time_var(base_dir):
    """Get TStore time variable."""
    from tstore.archive.metadata.readers import read_tstore_metadata

    metadata = read_tstore_metadata(base_dir=base_dir)
    return metadata["time_var"]


def get_id_var(base_dir):
    """Get TStore ID variable."""
    from tstore.archive.metadata.readers import read_tstore_metadata

    metadata = read_tstore_metadata(base_dir=base_dir)
    return metadata["id_var"]


def get_partitions(base_dir, ts_variable):
    """Get TStore time series partitioning."""
    from tstore.archive.metadata.readers import read_tstore_metadata

    metadata = read_tstore_metadata(base_dir=base_dir)
    partitioning = metadata["partitioning"][ts_variable]
    partitions = partitioning.split("/") if partitioning is not None else []
    return partitions


def get_ts_info(
    base_dir: Path | str,
    ts_variable: str,
    var_prefix: str,
):
    """
    Retrieve filepaths and tstore_ids for a specific ts_variable.

    Parameters
    ----------
    base_dir : path-like
        Base directory of the TStore.
    ts_variable : str
        Name of the time series variable.
    var_prefix : str
        Prefix for the variable directory in the TStore.

    Returns
    -------
    fpaths : list of str
        List of filepaths for the time series.
    tstore_ids : list of str
        List of time series IDs.
    partitions : list of str
        List of partitions.
    """
    tstore_structure = get_tstore_structure(base_dir)

    # TODO: DRY with `define_tsarray_filepath`?
    var_dir_basename = f"{var_prefix}={ts_variable}"

    if tstore_structure == "id-var":
        fpaths = glob.glob(os.path.join(base_dir, "*", var_dir_basename))
        tstore_ids = [os.path.basename(os.path.dirname(fpath)) for fpath in fpaths]
    elif tstore_structure == "var-id":
        fpaths = glob.glob(os.path.join(base_dir, var_dir_basename, "*"))
        tstore_ids = [os.path.basename(fpath) for fpath in fpaths]
    else:
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    # get only id values (remove prefix from hive prefix=value notation)
    tstore_ids = [tstore_id.split("=")[1] for tstore_id in tstore_ids]

    partitions = get_partitions(base_dir, ts_variable)

    return fpaths, tstore_ids, partitions
