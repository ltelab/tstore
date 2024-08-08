#!/usr/bin/env python3
"""
Created on Mon Apr  8 17:24:09 2024.

@author: ghiggi
"""

from typing import Optional

import yaml

from tstore.archive.io import define_metadata_filepath


def _write_yaml_metadata(metadata, fpath):
    """Write metadata YAML file."""
    with open(fpath, "w") as file:
        yaml.dump(metadata, file)


def write_tstore_metadata(
    base_dir: str,
    ts_vars: dict[str, list[str]],
    id_var: str,
    tstore_structure: str,
    partitioning: Optional[str] = None,
):
    """Write TStore metadata file."""
    metadata_fpath = define_metadata_filepath(base_dir)
    metadata = {}
    metadata["ts_variables"] = ts_vars
    metadata["id_var"] = id_var
    metadata["tstore_structure"] = tstore_structure
    metadata["partitioning"] = partitioning
    _write_yaml_metadata(metadata=metadata, fpath=metadata_fpath)
