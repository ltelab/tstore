#!/usr/bin/env python3
"""
Created on Mon Apr  8 16:31:23 2024.

@author: ghiggi
"""


def get_time_filters(
    start_time=None,
    end_time=None,
):
    """Define filters for Parquet Dataset subsetting at read-time."""
    filters = []
    if start_time is not None:
        filters.append(("time", ">=", start_time))
    if end_time is not None:
        filters.append(("time", "<=", end_time))

    if filters:
        return filters
    return None
