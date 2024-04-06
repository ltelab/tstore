#!/usr/bin/env python3
"""
Created on Mon Jun 12 15:35:06 2023.

@author: ghiggi
"""
import yaml

from tstore.archive.io import define_metadata_filepath


def _read_yaml_metadata(fpath):
    """Read metadata YAML file."""
    with open(fpath) as file:
        metadata = yaml.safe_load(file)
    return metadata


def read_metadata(base_dir):
    """Read TStore metadata."""
    metadata_fpath = define_metadata_filepath(base_dir)
    metadata = _read_yaml_metadata(metadata_fpath)
    return metadata
