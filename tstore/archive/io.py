"""TStore I/O tools."""

import glob
import os
import shutil


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


def define_tsarray_filepath(base_dir, tstore_id, ts_variable, tstore_structure):
    """Define filepath of a TStore TS."""
    if tstore_structure == "id-var":
        fpath = os.path.join(base_dir, tstore_id, ts_variable)
    elif tstore_structure == "var-id":
        fpath = os.path.join(base_dir, ts_variable, tstore_id)
    else:
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    return fpath


def define_metadata_filepath(base_dir):
    """Define TStore metadata filepath."""
    metadata_fpath = os.path.join(base_dir, "tstore_metadata.yml")
    return metadata_fpath


def get_tstore_structure(base_dir):
    """Get TStore structure."""
    from tstore.archive.readers import read_metadata

    metadata = read_metadata(base_dir=base_dir)
    return metadata["tstore_structure"]


def get_ts_info(base_dir, ts_variable):
    """Retrieve filepaths and tstore_ids for a specific ts_variable."""
    tstore_structure = get_tstore_structure(base_dir)
    if tstore_structure == "id-var":
        fpaths = glob.glob(os.path.join(base_dir, "*", ts_variable))
        tstore_ids = [os.path.basename(os.path.dirname(fpath)) for fpath in fpaths]
    elif tstore_structure == "var-id":
        fpaths = glob.glob(os.path.join(base_dir, ts_variable, "*"))
        tstore_ids = [os.path.basename(fpath) for fpath in fpaths]
    else:
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    return fpaths, tstore_ids
