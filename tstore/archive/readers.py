#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 15:35:06 2023

@author: ghiggi
"""
import yaml 
import pandas as pd
from tstore.archive.io import (
    define_metadata_filepath,
    define_attributes_filepath,
)


def _read_yaml_metadata(fpath): 
    """Read metadata YAML file."""
    with open(fpath, 'r') as file:
        metadata = yaml.safe_load(file)
    return metadata 


def read_metadata(base_dir):
    """Read TSTORE metadata."""
    metadata_fpath = define_metadata_filepath(base_dir)
    metadata = _read_yaml_metadata(metadata_fpath)
    return metadata


def read_attributes(base_dir):
    """Read TSTORE attributes."""
    # TODO: maybe enable to read also in others format --> pandas, dask, polars, pyarrow
    fpath = define_attributes_filepath(base_dir)
    df = pd.read_parquet(fpath, engine="pyarrow", dtype_backend="pyarrow")
    df.index = df.index.astype("string[pyarrow]")
    return df