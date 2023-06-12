"""TStore I/O tools.""" 
import os
import glob
import shutil

def check_tstore_structure(tstore_structure): 
    """Check validity of TSTORE structure."""
    if tstore_structure not in ['id-var', 'var-id']:
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    return tstore_structure         


def check_tstore_directory(base_dir, overwrite):
    """Check TSTORE directory."""
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
    """Define filepath of TSTORE attributes."""
    fpath = os.path.join(base_dir, "_attributes.parquet")
    return fpath 
    

def define_tsarray_filepath(base_dir, identifier, var, tstore_structure):
    """Define filepath of a TSTORE TS."""
    if tstore_structure == 'id-var':
        fpath = os.path.join(base_dir, identifier, var)
    elif tstore_structure == 'var-id':
        fpath = os.path.join(base_dir, var, identifier)
    else: 
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    return fpath


def define_metadata_filepath(base_dir): 
    """Define TSTORE metadata filepath."""
    metadata_fpath = os.path.join(base_dir, "tstore_metadata.yml")
    return metadata_fpath


def _get_ts_info(base_dir, var, tstore_structure):
    """Retrieve filepaths and identifiers for a specific timeseries."""
    if tstore_structure == 'id-var':
        fpaths = glob.glob(os.path.join(base_dir,"*", var))
        identifiers = [os.path.basename(os.path.dirname(fpath)) for fpath in fpaths]
    elif tstore_structure == 'var-id':
        fpaths = glob.glob(os.path.join(base_dir, var, "*"))
        identifiers = [os.path.basename(fpath) for fpath in fpaths]
    else: 
        raise ValueError("Valid tstore_structure are 'id-var' and 'var-id'.")
    return fpaths, identifiers