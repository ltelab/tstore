#!/usr/bin/env python3
"""
Created on Mon Jun 12 23:22:11 2023.

@author: ghiggi
"""

from typing import Optional

import geopandas as gpd

from tstore.archive.io import define_attributes_filepath


def read_geometry(base_dir: str, id_var: str) -> Optional[gpd.GeoDataFrame]:
    """Read TStore geometry in a GeoDataFrame."""
    path = define_attributes_filepath(base_dir)

    try:
        df = gpd.read_parquet(path)
    except ValueError:  # Not a GeoDataFrame
        return None

    df = df[[id_var, df.geometry.name]]

    return df
