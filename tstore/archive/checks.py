#!/usr/bin/env python3
"""
Created on Mon Jun 12 21:32:37 2023.

@author: ghiggi
"""
import datetime

import numpy as np


def get_available_ts_variables(base_dir):
    """Get available TStore timeseries."""
    from tstore.archive.readers import read_metadata

    metadata = read_metadata(base_dir=base_dir)
    return metadata["ts_variables"]


def check_is_tstore(base_dir):
    """Check is a TStore."""
    # TODO
    pass
    return base_dir


def check_ts_variables(ts_variables, base_dir):
    """Check valid ts_variables.

    If None, return all the available ts_variables.
    """
    available_ts_variables = get_available_ts_variables(base_dir)
    if isinstance(ts_variables, str):
        ts_variables = [ts_variables]
    if isinstance(ts_variables, type(None)):
        ts_variables = available_ts_variables
    ts_variables = np.array(ts_variables)
    unvalid_ts_variables = ts_variables[np.isin(ts_variables, available_ts_variables, invert=True)]
    ts_variables = ts_variables.tolist()
    unvalid_ts_variables = unvalid_ts_variables.tolist()
    if len(unvalid_ts_variables) > 0:
        raise ValueError(
            f"Valid ts_variables are {ts_variables}. Invalid: {unvalid_ts_variables}.",
        )
    return ts_variables


def check_tstore_ids(tstore_ids, base_dir):
    """Check valid tstore_ids.

    If tstore_ids=None, return None.
    """
    # TODO:
    pass
    return tstore_ids


def check_time(time):
    """Check time validity.

    It returns a datetime.datetime object.

    Parameters
    ----------
    time : (datetime.datetime, datetime.date, np.datetime64, str)
        Time object.
        Accepted types: datetime.datetime, datetime.date, np.datetime64, str
        If string type, it expects the isoformat 'YYYY-MM-DD hh:mm:ss'.

    Returns
    -------
    time : datetime.datetime
        datetime.datetime object.

    """
    # TODO: adapt to return the more appropriate object !!!
    if not isinstance(
        time,
        (datetime.datetime, datetime.date, np.datetime64, np.ndarray, str),
    ):
        raise TypeError(
            "Specify time with datetime.datetime objects or a " "string of format 'YYYY-MM-DD hh:mm:ss'.",
        )
    # If numpy array with datetime64 (and size=1)
    if isinstance(time, np.ndarray):
        if np.issubdtype(time.dtype, np.datetime64):
            if time.size == 1:
                time = time.astype("datetime64[ns]").tolist()
            else:
                raise ValueError("Expecting a single timestep!")
        else:
            raise ValueError("The numpy array does not have a np.datetime64 dtype!")
    # If np.datetime64, convert to datetime.datetime
    if isinstance(time, np.datetime64):
        time = time.astype("datetime64[s]").tolist()
    # If datetime.date, convert to datetime.datetime
    if not isinstance(time, (datetime.datetime, str)):
        time = datetime.datetime(time.year, time.month, time.day, 0, 0, 0)
    if isinstance(time, str):
        try:
            time = datetime.datetime.fromisoformat(time)
        except ValueError:
            raise ValueError("The time string must have format 'YYYY-MM-DD hh:mm:ss'")
    return time


def check_start_end_time(start_time, end_time):
    """Check start_time and end_time validity."""
    # Format time input
    if start_time is not None:
        start_time = check_time(start_time)
    if end_time is not None:
        end_time = check_time(end_time)
    # Check start_time and end_time are chronological
    if start_time is not None and end_time is not None and start_time > end_time:
        raise ValueError("Provide start_time occurring before of end_time")
    return (start_time, end_time)
