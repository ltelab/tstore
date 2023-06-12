#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 16:41:25 2023.

@author: ghiggi
"""
import geopandas as gpd

fpath = "/home/ghiggi/Python_Packages/tstore/data/processed/variables/temperature.gpkg"
data = gpd.read_file(fpath)

# ERROR: Does not open !
